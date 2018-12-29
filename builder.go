package rithdb

import "fmt"

type SqlAble interface {
	ToSql() string
}

type Value struct {
	v interface{}
}

func (va *Value) GetString() string{
	if s,ok := va.v.(string); ok {
		return s
	}
	if s,ok := va.v.([]byte); ok {
		return string(s)
	}
	return ""
}

func (va *Value) String() string{
	return va.GetString()
}

type Constraint struct {
	kind string
	operator string
	connector string
}

type QueryBuilder struct {
	connection *Connection
	columns []string
	from string
	wheres map[string][3]string
	bindings map[string][]Value
	limit int
	offset int
}

func (q *QueryBuilder) Where(column string, operator string, value interface{}, and bool) *QueryBuilder {
	var b string
	if and {
		b = "and"
	} else {
		b = "or"
	}
	q.wheres[column] = [3]string{"basic", operator, b}
	q.bindings["where"] = append(q.bindings["where"], Value{value})
	
	return q
}


func (q *QueryBuilder) From(table string) *QueryBuilder {

	q.from = table
	return q
}

func (q *QueryBuilder) GetRaw(query string, bindings... interface{}) *Collection {

	rows, err := q.connection.Select(query, bindings...)

	if err != nil {
		panic(err)
	}
	return Collect(rows)
}

func (q *QueryBuilder) Get(columns... string) *Collection {
	if len(q.columns) < 1 {
		q.columns = columns
	}

	sql := q.ToSql()

	// TODO: dispatch sql event
	fmt.Println(sql)
	return q.GetRaw(sql, q.FlatBindings()...)
}

func (q *QueryBuilder) First(columns... string) Item {
	return q.Take(1).Get().First()
}

func (q *QueryBuilder) FlatBindings() []interface{} {
	var bs []interface{}

	for _, v := range q.bindings {
		for _, b := range v {
			bs = append(bs, b.v)
		}
	}

	return bs
}

func (q *QueryBuilder) Select(columns... string) *QueryBuilder {
	q.columns = columns
	return q
}

func (q *QueryBuilder) Take(limit int) *QueryBuilder {
	q.limit = limit
	return q
}

func (q *QueryBuilder) Offset(offset int) *QueryBuilder {
	q.offset = offset
	return q
}

func (q *QueryBuilder) ToSql() string {
	return q.connection.grammar.Compile(q)
}

func (q *QueryBuilder) GetColumns() []string {
	return q.columns
}


func (q *QueryBuilder) GetConnection() *Connection {
	return q.connection
}

func NewValue(v interface{}) Value {
	return Value{v}
}

func NewBuilder(c *Connection) *QueryBuilder{
	q := new(QueryBuilder)
	q.connection = c
	return q
}

