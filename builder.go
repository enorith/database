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
	inLens []int
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

func (q *QueryBuilder) Exists() bool {
	sql := q.connection.grammar.CompileExists(q)
	rows, err := q.connection.Select(sql, q.FlatBindings()...)
	if err != nil {
		return false
	}
	var exists bool
	rows.Next()
	rows.Scan(&exists)
	rows.Close()

	return exists
}

func (q *QueryBuilder) Count(column ...string) int64 {

	sql := q.connection.grammar.CompileCount(q, column...)

	rows, err := q.connection.Select(sql, q.FlatBindings()...)
	if err != nil {
		return 0
	}
	var aggregate int64
	rows.Next()
	rows.Scan(&aggregate)
	rows.Close()

	return aggregate
}

func (q *QueryBuilder) WhereIn(column string, value []interface{}, and bool) *QueryBuilder {
	q.addWhere(whereIn, column, "in", and)
	q.bindings = append(q.bindings, value)
	q.inLens = append(q.inLens, len(value))

	return q
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

func (q *QueryBuilder) GetRows(columns ... string) (*RowsIterator, error) {
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
	var value []interface{}

	for _, v := range q.bindings {
		if inValue, ok := v.([]interface{}); ok {
			value = append(value, inValue...)
		} else if betweenValue, ok := v.([2]interface{}); ok {
			value = append(value, betweenValue[0], betweenValue[1])
		} else {
			value = append(value, v)
		}
	}

	return value
}

func (q *QueryBuilder) Create(attributes map[string]interface{}, key ...string) (CollectionItem, error) {
	sql,bindings := q.connection.grammar.CompileInsertOne(q.from, attributes)

	id, err := q.connection.InsertGetId(sql, bindings...)
	if err != nil {
		return CollectionItem{}, err
	}
	var primary string

	if id > 0 {
		if len(key) > 0 {
			primary = key[0]
		} else {
			primary = "id"
		}
	}

	found, findErr := q.AndWhere(primary, "=", id).First()
	if findErr != nil {
		return CollectionItem{}, err
	}

	return found, nil
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
