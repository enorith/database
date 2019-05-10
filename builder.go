package rithdb

import "fmt"

type SqlAble interface {
	ToSql() string
}

type Value struct {
	v interface{}
}

var (
	whereBasic   = "b"
	whereSub     = "s"
	whereNest    = "x"
	whereNull    = "n"
	whereIn      = "i"
	whereBetween = "t"
)

type QueryHandler func(builder *QueryBuilder)

func (va *Value) GetString() string {
	if s, ok := va.v.(string); ok {
		return s
	}
	if s, ok := va.v.([]byte); ok {
		return string(s)
	}
	return ""
}

func (va *Value) String() string {
	return va.GetString()
}

type Constraint struct {
	kind      string
	operator  string
	connector string
}

type QueryBuilder struct {
	connection *Connection
	columns    []string
	from       string

	wheres   [][4]string
	bindings []interface{}
	// Do not use map
	orders [][2]string
	limit  int
	offset int
}

func (q *QueryBuilder) Where(column, operator string, value interface{}, and bool) *QueryBuilder {
	q.addWhere(whereBasic, column, operator, and)
	q.bindings = append(q.bindings, value)

	return q
}

func (q *QueryBuilder) addWhere(typ, column, operator string, and bool) *QueryBuilder {
	var b string
	if and {
		b = "and"
	} else {
		b = "or"
	}

	q.wheres = append(q.wheres, [4]string{column, typ, operator, b})

	return q
}

func (q *QueryBuilder) WhereNull(column string, and bool) *QueryBuilder {

	q.addWhere(whereNull, column, "is null", and)

	return q
}

func (q *QueryBuilder) WhereNotNull(column string, and bool) *QueryBuilder {

	q.addWhere(whereNull, column, "is not null", and)

	return q
}

func (q *QueryBuilder) WhereNest(and bool, handler QueryHandler) *QueryBuilder {
	builder := q.NewQuery()
	handler(builder)

	sql := q.connection.grammar.CompileWheres(builder, false)

	q.bindings = append(q.bindings, builder.bindings...)

	q.addWhere(whereNest, "("+sql+")", "", and)

	return q
}

func (q *QueryBuilder) AndWhereNest(handler QueryHandler) *QueryBuilder {
	return q.WhereNest(true, handler)
}

func (q *QueryBuilder) WhereSub(from, column, operator string, and bool, handler QueryHandler) *QueryBuilder {
	builder := q.NewQuery()

	handler(builder)
	builder.From(from)
	sql := q.connection.grammar.Compile(builder)

	q.bindings = append(q.bindings, builder.bindings...)

	q.addWhere(whereSub, column, fmt.Sprintf("%s (%s)", operator, sql), and)
	return q
}


func (q *QueryBuilder) AndWhereSub(from, column, operator string, handler QueryHandler) *QueryBuilder {
	return q.WhereSub(from, column, operator, true, handler)
}

func (q *QueryBuilder) AndWhere(column string, operator string, value interface{}) *QueryBuilder {
	return q.Where(column, operator, value, true)
}

func (q *QueryBuilder) OrWhere(column string, operator string, value interface{}) *QueryBuilder {
	return q.Where(column, operator, value, false)
}

func (q *QueryBuilder) From(table string) *QueryBuilder {

	q.from = table
	return q
}

func (q *QueryBuilder) GetRaw(query string, bindings ... interface{}) (*Collection, error) {

	rows, err := q.connection.Select(query, bindings...)

	if err != nil {
		return nil, err
	}

	return CollectRows(rows)
}

func (q *QueryBuilder) Get(columns ... string) (*Collection, error) {
	if len(q.columns) < 1 {
		q.columns = columns
	}

	return q.GetRaw(q.ToSql(), q.FlatBindings()...)
}

func (q *QueryBuilder) SelectRows(columns ... string) (*RowsIterator, error) {
	if len(q.columns) < 1 {
		q.columns = columns
	}
	rows, err := q.connection.Select(q.ToSql(), q.FlatBindings()...)

	if err != nil {
		return nil, err
	}

	return NewRowsIterator(rows)
}

func (q *QueryBuilder) Sort(by string, direction string) *QueryBuilder {
	q.orders = append(q.orders, [2]string{by, direction})

	return q
}
func (q *QueryBuilder) SortDesc(by string) *QueryBuilder {
	return q.Sort(by, "desc")
}

func (q *QueryBuilder) SortAsc(by string) *QueryBuilder {
	return q.Sort(by, "asc")
}

func (q *QueryBuilder) First(columns ... string) (CollectionItem, error) {
	coll, err := q.Take(1).Get(columns...)
	if err != nil {
		return CollectionItem{}, err
	}

	return coll.First(), nil
}

func (q *QueryBuilder) FlatBindings() []interface{} {
	return q.bindings
}

func (q *QueryBuilder) Select(columns ... string) *QueryBuilder {
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

func (q *QueryBuilder) ForPage(page int, perPage int) *QueryBuilder {
	return q.Offset((page - 1) * perPage).Take(perPage)
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

func (q *QueryBuilder) NewQuery() *QueryBuilder {
	return NewBuilder(q.connection.Clone())
}

func NewValue(v interface{}) Value {
	return Value{v}
}

func NewBuilder(c *Connection) *QueryBuilder {
	q := new(QueryBuilder)
	q.connection = c
	q.orders = [][2]string{}
	q.bindings = []interface{}{}
	return q
}
