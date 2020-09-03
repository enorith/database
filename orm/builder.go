package orm

import (
	"fmt"
	"github.com/enorith/database"
	"github.com/jinzhu/inflection"
	"reflect"
	"strings"
	"sync"
)

type ModelNotfoundError string

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

func (b *Builder) Marshal(models interface{}) error {
	table := guessTableName(models)
	collection, e := b.From(table).Get()

	if e != nil {
		return e
	}

	return b.marshalModels(models, collection)
}

func (b *Builder) FindFor(id int64, model interface{}) error {
	table := guessTableName(model)
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
	t := typeStruct(v)
	o := valueStruct(v)

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
		t = typeStruct(v)
	}

	o := valueStruct(v)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.Anonymous {
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

func typeStruct(v interface{}) reflect.Type {
	if t, ok := v.(reflect.Type); ok {
		return t
	}

	t := reflect.TypeOf(v)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}

func valueStruct(v interface{}) reflect.Value {
	if va, ok := v.(reflect.Value); ok {
		return va
	}

	va := reflect.ValueOf(v)

	if va.Kind() == reflect.Ptr {
		va = va.Elem()
	}

	return va
}

func guessKeyName(v interface{}) string {
	it := typeStruct(v)

	cacheKey := "keyName:" + it.String()

	if keyName, ok := tc.get(cacheKey); ok {
		return keyName
	}

	var key string
	iv := reflect.New(it).Interface()
	if tm, ok := iv.(KeyedModel); ok {
		key = tm.GetKeyName()
	} else {
		key = "id"
	}
	tc.set(cacheKey, key)

	return key
}

func guessTableName(v interface{}) string {
	t := typeStruct(v)

	var table string

	cacheKey := "table:" + t.String()

	if table, ok := tc.get(cacheKey); ok {
		return table
	}

	var it reflect.Type
	switch t.Kind() {
	case reflect.Slice:
		it = t.Elem()
		fallthrough
	case reflect.Struct:
		it = t
		iv := reflect.New(it).Interface()
		if tm, ok := iv.(TabledModel); ok {
			table = tm.GetTable()
		} else {
			typeName := it.String()
			ns := strings.Split(typeName, ".")
			table = inflection.Plural(strings.ToLower(ns[len(ns)-1]))
		}
	}

	if table != "" {
		tc.set(cacheKey, table)
	}

	return table
}

func init() {
	tc = &tableCaches{map[string]string{}, sync.RWMutex{}}
}
