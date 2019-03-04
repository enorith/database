package rithdb

import (
	"fmt"
	"strings"
)

var grammars map[string]Grammar

type Grammar interface {
	Compile(s *QueryBuilder) string
}

// SqlGrammar is sql compiler
// compile QueryBuilder to sql string
type SqlGrammar struct {
}

func (g *SqlGrammar) Compile(s *QueryBuilder) string {
	sql := g.compileColumns(s) +
		g.compileFrom(s) +
		g.compileWheres(s) +
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

func (g *SqlGrammar) compileWheres(s *QueryBuilder) string {
	where := ""

	for k, w := range s.wheres {
		var appendStr string
		if k != 0 {
			appendStr = w[3] + " "
		}
		where += fmt.Sprintf("%s%s %s ? ", appendStr, w[0], w[2])
	}

	if len(where) > 0 {
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
