package database

import (
	"fmt"
	"time"
)

var (
	whereBasic   = "b"
	whereSub     = "s"
	whereNest    = "x"
	whereNull    = "l"
	whereNotNull = "n"
	whereIn      = "i"
	whereBetween = "t"
	whereColumn  = "c"
)

var DefaultPerPage = 15

type QueryHandler func(builder *QueryBuilder)
type JoinHandler func(clause *JoinClause)

type QueryBuilder struct {
	connection *Connection
	columns    []string
	from       string

	wheres   [][5]string
	bindings []interface{}
	// Do not use map
	orders [][2]string
	groups []string
	limit  int
	offset int
	inLens []int
	joins  []*JoinClause
}

func (q *QueryBuilder) Where(column, operator string, value interface{}, and bool) *QueryBuilder {
	q.addWhere(whereBasic, column, operator, and)
	q.bindings = append(q.bindings, value)

	return q
}

func (q *QueryBuilder) addWhere(typ, column, operator string, and bool, others ...string) *QueryBuilder {
	var b string
	if and {
		b = "and"
	} else {
		b = "or"
	}
	var other = ""
	if len(others) > 0 {
		other = others[0]
	}

	q.wheres = append(q.wheres, [5]string{column, typ, operator, b, other})

	return q
}

func (q *QueryBuilder) WhereNull(column string, and bool) *QueryBuilder {

	q.addWhere(whereNull, column, "", and)

	return q
}

func (q *QueryBuilder) AndWhereNull(column string) *QueryBuilder {

	q.WhereNull(column, true)

	return q
}

func (q *QueryBuilder) WhereNotNull(column string, and bool) *QueryBuilder {

	q.addWhere(whereNotNull, column, "", and)

	return q
}

func (q *QueryBuilder) AndWhereNotNull(column string) *QueryBuilder {

	q.WhereNotNull(column, true)

	return q
}

func (q *QueryBuilder) WhereNest(and bool, handler QueryHandler) (*QueryBuilder, error) {
	builder := q.NewQuery()
	handler(builder)

	grammar, err := q.connection.GetGrammar()
	if err != nil {
		return builder, err
	}

	sql := grammar.CompileWheres(builder, false)

	q.bindings = append(q.bindings, builder.bindings...)

	q.addWhere(whereNest, sql, "", and)

	return q, nil
}

func (q *QueryBuilder) AndWhereNest(handler QueryHandler) (*QueryBuilder, error) {
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

	grammar, e := q.connection.GetGrammar()
	if e != nil {
		return 0
	}
	sql := grammar.CompileCount(q, column...)

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

func (q *QueryBuilder) WhereBetween(column string, one interface{}, two interface{}, and bool) *QueryBuilder {
	q.addWhere(whereBetween, column, "between", and)
	q.bindings = append(q.bindings, [2]interface{}{one, two})

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

func (q *QueryBuilder) FromSub(builder *QueryBuilder, as string) (*QueryBuilder, error) {

	toSql, err := builder.ToSql()

	if err != nil {
		return nil, err
	}

	bindings := builder.FlatBindings()

	q.from = Raw(fmt.Sprintf("(%s) as %s", toSql, WrapValue(as)))
	q.bindings = append(q.bindings, bindings...)

	return q, nil
}

func (q *QueryBuilder) GetRaw(query string, bindings ...interface{}) (*Collection, error) {

	rows, err := q.connection.Select(query, bindings...)

	if err != nil {
		return nil, err
	}

	return CollectRows(rows)
}

func (q *QueryBuilder) Get(columns ...string) (*Collection, error) {
	if len(q.columns) < 1 {
		q.columns = columns
	}

	sql, err := q.ToSql()

	if err != nil {
		return nil, err
	}

	return q.GetRaw(sql, q.FlatBindings()...)
}

func (q *QueryBuilder) GetRowsIterator(columns ...string) (*RowsIterator, error) {
	if len(q.columns) < 1 {
		q.columns = columns
	}
	sql, e := q.ToSql()
	if e != nil {
		return nil, e
	}

	rows, err := q.connection.Select(sql, q.FlatBindings()...)

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

func (q *QueryBuilder) First(columns ...string) (*CollectionItem, error) {
	coll, err := q.Take(1).Get(columns...)
	if err != nil {
		return &CollectionItem{}, err
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

func (q *QueryBuilder) Create(attributes map[string]interface{}, key ...string) (*CollectionItem, error) {
	sql, bindings := q.connection.grammar.CompileInsertOne(q.from, attributes)

	id, err := q.connection.InsertGetId(sql, bindings...)
	if err != nil {
		return &CollectionItem{}, err
	}
	var primary string

	if id > 0 {
		if len(key) > 0 {
			primary = key[0]
		} else {
			primary = "id"
		}
	}

	found, findErr := NewBuilder(q.connection).From(q.from).AndWhere(primary, "=", id).First()
	if findErr != nil {
		return &CollectionItem{}, findErr
	}

	return found, nil
}

func (q *QueryBuilder) Select(columns ...string) *QueryBuilder {
	q.columns = columns
	return q
}

func (q *QueryBuilder) Join(table, first, operator, second, category string) *QueryBuilder {
	q.JoinWith(category, table, func(clause *JoinClause) {
		clause.On(first, operator, second, true)
	})

	return q
}

func (q *QueryBuilder) LeftJoin(table, first, operator, second string) *QueryBuilder {
	q.JoinWith("left", table, func(clause *JoinClause) {
		clause.On(first, operator, second, true)
	})

	return q
}

func (q *QueryBuilder) RightJoin(table, first, operator, second string) *QueryBuilder {
	q.JoinWith("right", table, func(clause *JoinClause) {
		clause.On(first, operator, second, true)
	})

	return q
}

func (q *QueryBuilder) InnerJoin(table, first, operator, second string) *QueryBuilder {
	q.JoinWith("inner", table, func(clause *JoinClause) {
		clause.On(first, operator, second, true)
	})

	return q
}

func (q *QueryBuilder) JoinWith(category, table string, handler JoinHandler) *QueryBuilder {
	clause := &JoinClause{q.NewQuery(), category, table}
	handler(clause)

	q.joins = append(q.joins, clause)
	q.bindings = append(q.bindings, clause.bindings...)

	return q
}

func (q *QueryBuilder) Transaction(handler func(builder *QueryBuilder) error) error {
	return q.connection.TransactionCall(func() error {
		return handler(q.NewQuery())
	})
}

func (q *QueryBuilder) Take(limit int) *QueryBuilder {
	q.limit = limit
	return q
}

func (q *QueryBuilder) Offset(offset int) *QueryBuilder {
	q.offset = offset
	return q
}

func (q *QueryBuilder) GroupBy(columns ...string) *QueryBuilder {
	q.groups = append(q.groups, columns...)
	return q
}

func (q *QueryBuilder) ForPage(page, perPage int) *QueryBuilder {
	return q.Offset((page - 1) * perPage).Take(perPage)
}

func (q *QueryBuilder) CountForPage(column ...string) int64 {
	builder := q.Clone()
	builder.limit = -1
	builder.offset = -1
	//builder.groups = []string{}
	builder.orders = [][2]string{}
	builder.Select(column...)
	query := q.NewQuery()
	sub, err := query.FromSub(builder, "page_count")
	if err != nil {
		return 0
	}

	return sub.Count(column...)
}

func (q *QueryBuilder) Paginate(page, perPage int) *LengthAwarePaginator {
	if page < 1 {
		page = 1
	}

	if perPage < 1 {
		perPage = DefaultPerPage
	}

	return &LengthAwarePaginator{
		q,
		page,
		perPage,
		-1,
		nil,
	}
}

func (q *QueryBuilder) ToSql() (string, error) {
	g, e := q.connection.GetGrammar()

	if e != nil {
		return "", e
	}

	return g.Compile(q), nil
}

func (q *QueryBuilder) Using(connection string) error {
	var e error
	q.connection, e = DefaultManager.GetConnection(connection)

	if e != nil {
		return e
	}

	return nil
}

func (q *QueryBuilder) Remember(key string, d time.Duration) (*Collection, error) {
	if Cache != nil {
		if Cache.Has(key) {
			coll := &Collection{}
			Cache.Get(key, coll)

			return coll, nil
		}

		co, err := q.Get()
		if err != nil {
			return nil, err
		}

		Cache.Put(key, co, d)

		return co, nil
	}

	return q.Get()
}

func (q *QueryBuilder) GetColumns() []string {
	return q.columns
}

func (q *QueryBuilder) GetConnection() (*Connection, error) {
	if q.connection == nil {
		connection, err := DefaultManager.GetConnection()
		if err != nil {
			return nil, err
		}
		q.connection = connection
	}

	return q.connection, nil
}

func (q *QueryBuilder) NewQuery() *QueryBuilder {
	return NewBuilder(q.connection.Clone())
}

func (q *QueryBuilder) Clone() *QueryBuilder {
	return &QueryBuilder{
		connection: q.connection,
		columns:    q.columns,
		from:       q.from,
		wheres:     q.wheres,
		bindings:   q.bindings,
		orders:     q.orders,
		groups:     q.groups,
		limit:      q.limit,
		offset:     q.offset,
		inLens:     q.inLens,
		joins:      q.joins,
	}
}

type JoinClause struct {
	*QueryBuilder
	category string
	table    string
}

func (j *JoinClause) On(first, operator, second string, and bool) *JoinClause {
	j.addWhere(whereColumn, first, operator, and, second)
	return j
}

func (j *JoinClause) AndOn(first, operator, second string) *JoinClause {
	return j.On(first, operator, second, true)
}
func (j *JoinClause) OrOn(first, operator, second string) *JoinClause {
	return j.On(first, operator, second, false)
}

func NewBuilder(c *Connection) *QueryBuilder {
	q := new(QueryBuilder)
	q.connection = c
	q.orders = [][2]string{}
	q.bindings = []interface{}{}
	q.offset = -1
	q.limit = -1
	return q
}
