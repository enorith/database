package rithdb

import (
	"bytes"
	"fmt"
	"github.com/CaoJiayuan/goutilities/str"
	"strings"
)

var grammars map[string]Grammar

var RawPrefix = '~'

type Grammar interface {
	Compile(s *QueryBuilder) string
	CompileWheres(s *QueryBuilder, withKeyword bool) string
	CompileExists(s *QueryBuilder) string
	CompileCount(s *QueryBuilder, column ...string) string
	CompileInsertOne(table string, data map[string]interface{}) (sql string, bindings []interface{})
}

// SqlGrammar is sql compiler
// compile QueryBuilder to sql string
type SqlGrammar struct {
}

func (g *SqlGrammar) Compile(s *QueryBuilder) string {
	sql := g.compileColumns(s) +
		g.compileFrom(s) +
		g.compileJoins(s) +
		g.CompileWheres(s, true) +
		g.compileGroups(s) +
		g.compileOrders(s) +
		g.compileLimit(s) +
		g.compileOffset(s)

	return sql
}

func (g *SqlGrammar) compileColumns(s *QueryBuilder) string {
	var col string
	if len(s.columns) < 1 {
		col = "* "
	} else {
		col = strings.Join(g.wrapColumns(s.columns), ", ") + " "
	}

	return "select " + col
}

func (g *SqlGrammar) compileFrom(s *QueryBuilder) string {
	return fmt.Sprintf("from %s ", WrapValue(s.from))
}

func (g *SqlGrammar) CompileExists(s *QueryBuilder) string {
	return fmt.Sprintf("select exists(%s) as `exists`", s.ToSql())
}

func (g *SqlGrammar) CompileCount(s *QueryBuilder, column ...string) string {
	var col string
	if len(column) > 0 {
		col = fmt.Sprintf("count(%s) as `aggregate`", WrapValue(column[0]))
	} else {
		col = "count(*) as `aggregate`"
	}

	return s.Select(Raw(col)).ToSql()
}

func (g *SqlGrammar) CompileInsertOne(table string, data map[string]interface{}) (sql string, bindings []interface{}) {
	var (
		cols      []string
		values    []interface{}
		countAttr int
	)
	for k, v := range data {
		cols = append(cols, k)
		values = append(values, v)
		countAttr++
	}
	placeholder := strings.Join(str.Duplicate("?", countAttr), ",")

	return fmt.Sprintf("insert into `%s`(`%s`) values(%s)",
		table, strings.Join(cols, "`,`"), placeholder), values
}

func (g *SqlGrammar) CompileWheres(s *QueryBuilder, withKeyword bool) string {
	where := ""
	inIndex := 0

	for k, w := range s.wheres {
		var (
			andOr       string
			placeholder string
		)
		whereType := w[1]
		operator := w[2]
		column := w[0]
		other := w[4]
		if k != 0 {
			andOr = w[3] + " "
		}

		if whereType == whereBasic {
			placeholder = "? "
		}
		if whereType == whereIn {
			placeholder = "(" + strings.Join(str.Duplicate("?", s.inLens[inIndex]), ",") + ") "
			inIndex++
		}
		if whereType == whereBetween {
			placeholder = "? and ? "
		}

		if whereType == whereColumn {
			placeholder = WrapValue(other) + " "
		}

		if whereType == whereNest {
			column = Raw("(" + column + ")")
		}

		if whereType == whereNull {
			operator = "is null"
			placeholder = " "
		}

		if whereType == whereNotNull {
			operator = "is not null"
			placeholder = " "
		}

		where += fmt.Sprintf("%s%s %s %s", andOr, WrapValue(column), operator, placeholder)
	}

	if len(where) > 0 && withKeyword {
		where = "where " + where
	}
	return where
}

func (g *SqlGrammar) compileOrders(s *QueryBuilder) string {
	if len(s.orders) < 1 {
		return ""
	}
	var orders []string

	for _, v := range s.orders {
		orders = append(orders, fmt.Sprintf("%s %s", WrapValue(v[0]), v[1]))
	}

	return fmt.Sprintf("order by %s ", strings.Join(orders, ", "))
}

func (g *SqlGrammar) compileLimit(s *QueryBuilder) string {
	if s.limit < 0 {
		return ""
	}
	return fmt.Sprintf("limit %d ", s.limit)
}

func (g *SqlGrammar) compileOffset(s *QueryBuilder) string {
	if s.offset < 0 || s.limit < 0 {
		return ""
	}
	return fmt.Sprintf("offset %d ", s.offset)
}

func (g *SqlGrammar) compileGroups(s *QueryBuilder) string {
	if len(s.groups) < 1 {
		return ""
	}

	return fmt.Sprintf("group by %s ", strings.Join(g.wrapColumns(s.groups), ","))
}

func (g *SqlGrammar) compileJoins(s *QueryBuilder) string {
	if len(s.joins) < 1 {
		return ""
	}

	var result = ""

	for _, join := range s.joins {
		wheres := g.CompileWheres(join.QueryBuilder, false)

		tab := WrapValue(join.table)
		joinString := tab
		if len(join.joins) > 0 {
			joinString = fmt.Sprintf("(%s %s)", tab, g.compileJoins(join.QueryBuilder))
		}

		result += fmt.Sprintf("%s join %s on %s", join.category, joinString, wheres)
	}

	return result
}

func (g *SqlGrammar) wrapColumns(columns []string) []string {

	var result []string
	for _, v := range columns {
		result = append(result, WrapValue(v))
	}

	return result
}

func WrapValue(value string) string {
	b := []byte(value)

	if b[0] == byte(RawPrefix) {
		return string(b[1:])
	}

	if bytes.Contains(b, []byte(" as ")) {
		partials := bytes.SplitN(b, []byte(" as "), 2)

		return string(bytes.Join([][]byte{wrapBytes(partials[0]), wrapBytes(partials[1])}, []byte(" as ")))
	}

	return string(wrapBytes(b))
}

func Raw(value string) string {
	return string(RawPrefix) + value
}

func wrapBytes(b []byte) []byte {
	var result = []byte("`")
	if bytes.IndexByte(b, '.') > -1 {
		bs := bytes.SplitN(b, []byte("."), 2)
		byb := bytes.Join(bs, []byte("`.`"))
		result = append(result, byb...)
	} else {
		result = append(result, b...)
	}

	return append(result, '`')
}

type MysqlGrammar struct {
	SqlGrammar
}

func RegisterGrammar(name string, g Grammar) {
	if grammars == nil {
		grammars = make(map[string]Grammar)
	}
	grammars[name] = g
}
