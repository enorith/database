package rithdb


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

func (q *QueryBuilder) Get(columns... []string)  {
	
}


func (q *QueryBuilder) ToSql() string {
	panic("implement me")
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

