package orm

import (
	"fmt"
	"github.com/enorith/database"
	"github.com/enorith/supports/reflection"
	"github.com/jinzhu/inflection"
	"reflect"
	"strings"
	"sync"
)

type ModelNotfoundError string

func (m ModelNotfoundError) StatusCode() int {
	return 404
}

func (m ModelNotfoundError) Error() string {
	return fmt.Sprintf("model of %s not found", string(m))
}

var tc *tableCaches

type tableCaches struct {
	c  map[string]string
	mu sync.RWMutex
}

func (tc *tableCaches) get(key string) (table string, ok bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	table, ok = tc.c[key]
	return
}

func (tc *tableCaches) set(key string, table string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.c[key] = table
}

type Builder struct {
	*database.QueryBuilder
}

func (b *Builder) From(table string) *Builder {
	b.QueryBuilder.From(table)

	return b
}

func (b *Builder) Where(column, operator string, value interface{}, and bool) *Builder {
	b.QueryBuilder.Where(column, operator, value, and)
	return b
}

func (b *Builder) WhereNull(column string, and bool) *Builder {
	b.QueryBuilder.WhereNull(column, and)

	return b
}

func (b *Builder) AndWhereNull(column string) *Builder {
	b.QueryBuilder.AndWhereNull(column)
	return b
}

func (b *Builder) AndWhereNotNull(column string) *Builder {
	b.QueryBuilder.AndWhereNotNull(column)

	return b
}

func (b *Builder) WhereNest(and bool, handler database.QueryHandler) (*Builder, error) {
	_, e := b.QueryBuilder.WhereNest(and, handler)

	return b, e
}

func (b *Builder) AndWhereNest(handler database.QueryHandler) (*Builder, error) {
	_, e := b.QueryBuilder.AndWhereNest(handler)

	return b, e
}

func (b *Builder) WhereIn(column string, value []interface{}, and bool) *Builder {
	b.QueryBuilder.WhereIn(column, value, and)

	return b
}

func (b *Builder) WhereBetween(column string, one interface{}, two interface{}, and bool) *Builder {
	b.QueryBuilder.WhereBetween(column, one, two, and)

	return b
}

func (b *Builder) WhereSub(from, column, operator string, and bool, handler database.QueryHandler) *Builder {
	b.QueryBuilder.WhereSub(from, column, operator, and, handler)

	return b
}

func (b *Builder) AndWhereSub(from, column, operator string, handler database.QueryHandler) *Builder {
	b.QueryBuilder.AndWhereSub(from, column, operator, handler)

	return b
}

func (b *Builder) AndWhere(column string, operator string, value interface{}) *Builder {
	b.QueryBuilder.AndWhere(column, operator, value)

	return b
}

func (b *Builder) OrWhere(column string, operator string, value interface{}) *Builder {
	b.QueryBuilder.OrWhere(column, operator, value)

	return b
}

func (b *Builder) FromSub(builder *Builder, as string) (*Builder, error) {

	_, e := b.QueryBuilder.FromSub(builder.QueryBuilder, as)
	if e != nil {
		return nil, e
	}

	return b, nil
}

func (b *Builder) Sort(by string, direction string) *Builder {
	b.QueryBuilder.Sort(by, direction)

	return b
}

func (b *Builder) SortDesc(by string) *Builder {
	b.QueryBuilder.SortDesc(by)

	return b
}

func (b *Builder) SortAsc(by string) *Builder {
	b.QueryBuilder.SortAsc(by)

	return b
}

func (b *Builder) Select(columns ...string) *Builder {
	b.QueryBuilder.Select(columns...)

	return b
}

func (b *Builder) Join(table, first, operator, second, category string) *Builder {
	b.QueryBuilder.Join(table, first, operator, second, category)

	return b
}

func (b *Builder) LeftJoin(table, first, operator, second string) *Builder {
	b.QueryBuilder.LeftJoin(table, first, operator, second)

	return b
}

func (b *Builder) RightJoin(table, first, operator, second string) *Builder {
	b.QueryBuilder.RightJoin(table, first, operator, second)

	return b
}

func (b *Builder) InnerJoin(table, first, operator, second string) *Builder {
	b.QueryBuilder.InnerJoin(table, first, operator, second)

	return b
}

func (b *Builder) JoinWith(category, table string, handler database.JoinHandler) *Builder {
	b.QueryBuilder.JoinWith(category, table, handler)

	return b
}

func (b *Builder) Take(limit int) *Builder {
	b.QueryBuilder.Take(limit)

	return b
}

func (b *Builder) Offset(offset int) *Builder {
	b.QueryBuilder.Offset(offset)

	return b
}

func (b *Builder) GroupBy(columns ...string) *Builder {
	b.QueryBuilder.GroupBy(columns...)

	return b
}

func (b *Builder) ForPage(page, perPage int) *Builder {
	b.QueryBuilder.ForPage(page, perPage)

	return b
}

func (b *Builder) Using(connection string) error {
	e := b.QueryBuilder.Using(connection)
	if e != nil {
		return e
	}

	return nil
}

func (b *Builder) Get(columns ...string) (*database.Collection, error) {
	return b.QueryBuilder.Get(columns...)
}

func (b *Builder) Marshal(models interface{}) error {
	table, e := guessTableName(models)
	if e != nil {
		return e
	}
	conn, e := guessConnection(models)
	if e != nil {
		return e
	}
	err := b.Using(conn)
	if err != nil {
		return err
	}

	collection, e := b.From(table).Get()

	if e != nil {
		return e
	}

	return b.marshalModels(models, collection)
}

func (b *Builder) FindFor(id interface{}, model interface{}) error {
	table, e := guessTableName(model)
	if e != nil {
		return e
	}
	conn, e := guessConnection(model)
	if e != nil {
		return e
	}
	err := b.Using(conn)
	if err != nil {
		return err
	}
	item, e := b.From(table).Where(guessKeyName(model), "=", id, true).First()

	if e != nil {
		return e
	}
	if !item.IsValid() {
		return ModelNotfoundError(table)
	}

	v := b.marshalModel(nil, model, item)

	if !v.IsValid() {
		return fmt.Errorf("unable to marshal model %v", model)
	}

	return nil
}

func (b *Builder) marshalModels(v interface{}, collection *database.Collection) error {
	t := reflection.StructType(v)
	o := reflection.StructValue(v)

	if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		it := t.Elem()
		iv := reflect.New(it)
		collection.Each(func(item *database.CollectionItem, index int) {
			o = reflect.Append(o, b.marshalModel(it, iv.Elem(), item))
		})
		out := reflect.ValueOf(v)
		if out.Kind() == reflect.Ptr {
			out = out.Elem()
		}
		out.Set(o)
		return nil
	}

	return fmt.Errorf("can not marshel %v to model slice", t)
}

func (b *Builder) marshalModel(t reflect.Type, v interface{}, item *database.CollectionItem) reflect.Value {
	if t == nil {
		t = reflection.StructType(v)
	}

	o := reflection.StructValue(v)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.Anonymous {
			// TODO: marshal relation
			input := f.Tag.Get("field")
			field := o.Field(i)
			if input != "" && field.CanSet() {
				switch f.Type.Kind() {
				case reflect.String:
					in, _ := item.GetString(input)
					field.SetString(in)
				case reflect.Int:
					fallthrough
				case reflect.Int32:
					fallthrough
				case reflect.Int64:
					in, _ := item.GetInt(input)
					field.SetInt(in)
				case reflect.Uint:
					fallthrough
				case reflect.Uint32:
					fallthrough
				case reflect.Uint64:
					in, _ := item.GetUint(input)
					field.SetUint(in)
				}
			}
		}
	}
	return o
}

func guessKeyName(v interface{}) string {
	it := reflection.StructType(v)

	cacheKey := "keyName:" + it.String()

	if keyName, ok := tc.get(cacheKey); ok {
		return keyName
	}

	var key string
	iv := reflect.New(it).Interface()
	if tm, ok := iv.(WithKey); ok {
		key = tm.KeyName()
	} else {
		key = "id"
	}
	tc.set(cacheKey, key)

	return key
}

func guessTableName(v interface{}) (string, error) {
	t := reflection.StructType(v)

	var table string

	cacheKey := "table:" + t.String()

	if table, ok := tc.get(cacheKey); ok {
		return table, nil
	}

	if tm, ok := v.(WithTable); ok {
		table = tm.Table()
	} else  {
		var it reflect.Type
		switch t.Kind() {
		case reflect.Slice:
			it = t.Elem()
		case reflect.Struct:
			it = t
		default:
			return "", fmt.Errorf("unable guess table name from %v", t)
		}
		iv := reflect.New(it).Interface()
		if tm, ok := iv.(WithTable); ok {
			table = tm.Table()
		} else {
			typeName := it.String()
			ns := strings.Split(typeName, ".")
			table = inflection.Plural(strings.ToLower(ns[len(ns)-1]))
		}
	}

	if table != "" {
		tc.set(cacheKey, table)
	}
	return table, nil
}

func guessConnection(v interface{}) (string, error) {
	t := reflection.StructType(v)

	var conn string

	cacheKey := "connection:" + t.String()

	if table, ok := tc.get(cacheKey); ok {
		return table, nil
	}
	if tm, ok := v.(WithConnection); ok  {
		conn = tm.Connection()
	} else {
		var it reflect.Type
		switch t.Kind() {
		case reflect.Struct:
			it = t
		case reflect.Slice:
			it = t.Elem()
		default:
			return "", fmt.Errorf("unable guess connection from %v", t)
		}
		iv := reflect.New(it).Interface()
		if tm, ok := iv.(WithConnection); ok {
			conn = tm.Connection()
		} else {
			conn = ""
		}
	}

	if conn != "" {
		tc.set(cacheKey, conn)
	}

	return conn, nil
}

func init() {
	tc = &tableCaches{map[string]string{}, sync.RWMutex{}}
}
