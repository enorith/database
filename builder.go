package rithdb

type SqlAble interface {
	ToSql() string
}

type Value struct {
	v interface{}
}

var (
	whereBasic = "basic"
	whereSub   = "sub"
	whereNest  = "nest"
	whereNull  = "null"
)

type WhereNestHandler func(builder *QueryBuilder)

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
	bindings map[string][]interface{}
	// Do not use map
	orders [][2]string
	limit  int
	offset int
}

func (q *QueryBuilder) Where(column string, operator string, value interface{}, and bool) *QueryBuilder {
	q.addWhere(whereBasic, column, operator, and)
	q.bindings["where"] = append(q.bindings["where"], value)

	return q
}

func (q *QueryBuilder) addWhere(typ string, column string, operator string, and bool) *QueryBuilder {
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

func (q *QueryBuilder) WhereNest(and bool, handler WhereNestHandler) *QueryBuilder {
	builder := NewBuilder(q.connection.Clone())
	handler(builder)

	sql := q.connection.grammar.CompileWheres(builder, false)

	for ty, value := range builder.bindings {
		q.bindings[ty] = append(q.bindings[ty], value...)
	}
	var b string
	if and {
		b = "and"
	} else {
		b = "or"
	}

	q.wheres = append(q.wheres, [4]string{"(" + sql + ")", whereNest, "", b})

	return q
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
		panic(err)
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
	coll, err := q.Take(1).Get()
	if err  != nil {
		return CollectionItem{}, err
	}

	return coll.First(), nil
}

func (q *QueryBuilder) FlatBindings() []interface{} {
	var bs []interface{}

	for _, v := range q.bindings {
		for _, b := range v {
			bs = append(bs, b)
		}
	}

	return bs
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

func NewValue(v interface{}) Value {
	return Value{v}
}

func NewBuilder(c *Connection) *QueryBuilder {
	q := new(QueryBuilder)
	q.connection = c
	q.orders = [][2]string{}
	q.bindings = map[string][]interface{}{}
	return q
}
