package rithdb

import (
	"fmt"
	"strings"
	"github.com/CaoJiayuan/goutilities/str"
)

var grammars map[string]Grammar

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
		g.CompileWheres(s, true) +
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
		col = strings.Join(s.columns, ", ") + " "
	}

	return "select " + col
}

func (g *SqlGrammar) compileFrom(s *QueryBuilder) string {
	return fmt.Sprintf("from `%s` ", s.from)
}

func (g *SqlGrammar) CompileExists(s *QueryBuilder) string {
	return fmt.Sprintf("select exists(%s) as `exists`", s.ToSql())
}

func (g *SqlGrammar) CompileCount(s *QueryBuilder, column ...string) string {
	var col string
	if len(column) > 0 {
		col = fmt.Sprintf("count(%s) as `aggregate`", column[0])
	} else {
		col = "count(*) as `aggregate`"
	}

	return s.Select(col).ToSql()
}

func (g *SqlGrammar) CompileInsertOne(table string, data map[string]interface{}) (sql string, bindings []interface{}) {
	var (
		cols []string
		values []interface{}
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
		if whereType == whereBasic ||
			whereType == whereNull ||
			whereType == whereSub ||
			whereType == whereIn ||
			whereType == whereBetween {
			if k != 0 {
				andOr = w[3] + " "
			}

			if whereType == whereBasic {
				placeholder = "? "
			}
			if whereType == whereIn {
				placeholder = "(" + strings.Join(str.Duplicate("?", s.inLens[inIndex]), ",") +") "
				inIndex++
			}
			if whereType == whereBetween {
				placeholder = "? and ? "
			}
		}

		where += fmt.Sprintf("%s%s %s %s", andOr, column, operator, placeholder)
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
		orders = append(orders, fmt.Sprintf("%s %s", v[0], v[1]))
	}

	return fmt.Sprintf("order by %s ", strings.Join(orders, ", "))
}

func (g *SqlGrammar) compileLimit(s *QueryBuilder) string {
	if s.limit < 1 {
		return ""
	}
	return fmt.Sprintf("limit %d ", s.limit)
}

func (g *SqlGrammar) compileOffset(s *QueryBuilder) string {
	if s.offset < 1 || s.limit < 1 {
		return ""
	}
	return fmt.Sprintf("offset %d ", s.offset)
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
