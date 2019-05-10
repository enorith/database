package rithdb

import (
	"database/sql"
	"encoding/json"
	//"github.com/CaoJiayuan/goutilities/str"
	//"strconv"
	"errors"
	"fmt"
	"github.com/CaoJiayuan/goutilities/str"
	"strconv"
	"reflect"
)

type ItemHolder func(item CollectionItem, index int)
type ItemFilter func(item CollectionItem, index int) bool

type TypeParser func(row map[string]interface{}, field string, columnType *sql.ColumnType, bytesData []byte)

var DefaultTypeParser TypeParser = parseType

type CollectionItem struct {
	item  map[string]interface{}
	valid bool
}

func (i *CollectionItem) ToJson() []byte {
	j, err := i.MarshalJSON()

	if err != nil {
		panic(err)
	}

	return j
}

func (i *CollectionItem) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.item)
}

func (i *CollectionItem) Original() map[string]interface{} {
	return i.item
}

func (i *CollectionItem) IsNil(key string) bool {
	v, err := i.GetValue(key)
	if err != nil {
		return true
	}

	return v == nil
}

func (i *CollectionItem) IsNotNil(key string) bool {
	return !i.IsNil(key)
}

func (i *CollectionItem) GetInt(key string) (int64, error) {
	v, err := i.GetValue(key)
	if err == nil {
		if i, ok := v.(int64); ok {
			return i, nil
		} else {
			return 0, errors.New(fmt.Sprintf("try to get int value from key [%s]", key))
		}
	}

	return 0, err
}

func (i *CollectionItem) GetUInt(key string) (uint64, error) {
	v, err := i.GetValue(key)
	if err == nil {
		if i, ok := v.(uint64); ok {
			return i, nil
		} else {
			return 0, errors.New(fmt.Sprintf("try to get int value from key [%s]", key))
		}
	}

	return 0, err
}

func (i *CollectionItem) GetString(key string) (string, error) {
	v, err := i.GetValue(key)
	if err == nil {
		if s, ok := v.(string); ok {
			return s, nil
		} else {
			return "", errors.New(fmt.Sprintf("try to get string value from key [%s]", key))
		}
	}

	return "", err
}

func (i *CollectionItem) GetValue(key string) (interface{}, error) {
	if v, exists := i.item[key]; exists {
		return v, nil
	}

	return nil, errors.New(fmt.Sprintf("map key [%s] not exists", key))
}

/// Collection is database rows collection
type Collection struct {
	items    []CollectionItem
	iterator *RowsIterator
	loaded   bool
}

func (c *Collection) MarshalJSON() ([]byte, error) {
	c.loadAll()
	return json.Marshal(c.items)
}

func (c *Collection) ToJson() []byte {
	j, err := c.MarshalJSON()

	if err != nil {
		panic(err)
	}

	return j
}

func (c *Collection) GetItem(key int) CollectionItem {
	c.loadAll()
	if len(c.items) < 1 {
		return CollectionItem{}
	}

	return c.items[key]
}

func (c *Collection) Each(resolver ItemHolder) {
	if len(c.items) > 0 {
		for k, v := range c.items {
			resolver(v, k)
		}
	}
	defer c.Close()
	var k int
	for c.Next() {
		resolver(c.Read(), k)
		k++
	}
}

func (c *Collection) First() CollectionItem {
	return c.GetItem(0)
}

func (c *Collection) GetItems() []CollectionItem {
	return c.items
}

func (c *Collection) Close() error {
	return c.iterator.Close()
}

func (c *Collection) Scan(dest ...interface{}) error {
	return c.iterator.Scan(dest...)
}

func (c *Collection) Next() bool {
	return c.iterator.Next()
}

func (c *Collection) Pluck(key string) []interface{} {
	var result []interface{}
	c.Each(func(item CollectionItem, index int) {
		v, _ := item.GetValue(key)
		result = append(result, v)
	})
	return result
}

func (c *Collection) PluckInt(key string) []int64 {
	var result []int64
	c.Each(func(item CollectionItem, index int) {
		v, _ := item.GetInt(key)
		result = append(result, v)
	})
	return result
}

//NextAndScan recommend way to get row
func (c *Collection) NextAndScan(dest ...interface{}) bool {
	return c.iterator.NextAndScan(dest...)
}

func (c *Collection) Read() CollectionItem {
	return CollectionItem{c.iterator.Read(), true}
}

func (c *Collection) loadAll() bool {
	if !c.loaded {
		defer c.Close()
		for c.Next() {
			c.items = append(c.items, c.Read())
		}
		c.loaded = true
		return true
	}

	return false
}

type RowsIterator struct {
	rows    *sql.Rows
	types   []*sql.ColumnType
	columns []string
}

func (i *RowsIterator) Next() bool {
	return i.rows.Next()
}

func (i *RowsIterator) Close() error {
	return i.rows.Close()
}
func (i *RowsIterator) Scan(dest ...interface{}) error {
	return i.rows.Scan(dest...)
}

func (i *RowsIterator) NextAndScan(dest ...interface{}) bool {
	hasNext := i.Next()
	err := i.Scan(dest...)
	if err != nil {
		return false
	}

	return hasNext
}

func (i *RowsIterator) Read() map[string]interface{} {
	length := len(i.columns)
	item := make([]interface{}, length)
	values := make([][]byte, length)
	for k := range values {
		item[k] = &values[k]
	}
	dataItem := map[string]interface{}{}
	i.rows.Scan(item...)

	for index, field := range i.columns {
		bytesData := values[index]
		DefaultTypeParser(dataItem, field, i.types[index], bytesData)
	}
	return dataItem
}

func CollectRows(rows *sql.Rows) (*Collection, error) {
	ite, err := NewRowsIterator(rows)
	if err != nil {
		return nil, err
	}

	return &Collection{
		iterator: ite,
	}, nil
}

func NewRowsIterator(rows *sql.Rows) (*RowsIterator, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	cols, colErr := rows.Columns()

	if colErr != nil {
		return nil, colErr
	}

	return &RowsIterator{
		rows:    rows,
		types:   types,
		columns: cols,
	}, err
}

func NewCollectionItem(data map[string]interface{}) CollectionItem {
	return CollectionItem{data, true}
}

func parseType(item map[string]interface{}, field string, columnType *sql.ColumnType, bytesData []byte) {
	typeName := columnType.DatabaseTypeName()
	if bytesData == nil {
		item[field] = nil
	} else if str.Contains(typeName, "INT") {
		unsigned := false
		size := 32
		strData := string(bytesData)
		scanType := columnType.ScanType()
		switch scanType.Kind() {
		case reflect.Uint8:
			unsigned = true
			size = 8
			break
		case reflect.Uint16:
			unsigned = true
			size = 16
			break
		case reflect.Uint:
		case reflect.Uint32:
			unsigned = true
			size = 32
			break
		case reflect.Uint64:
			unsigned = true
			size = 64
			break
		case reflect.Int8:
			unsigned = false
			size = 8
			break
		case reflect.Int16:
			unsigned = false
			size = 16
			break
		case reflect.Int:
		case reflect.Int32:
			unsigned = false
			size = 32
			break
		case reflect.Int64:
			unsigned = false
			size = 64
			break
		}

		if unsigned {
			unsignedInt,_ := strconv.ParseUint(strData, 10, size)
			item[field] = unsignedInt
		} else  {
			integer,_ := strconv.ParseInt(strData, 10, size)
			item[field] = integer
		}

	}  else if str.Contains(typeName, "CHAR", "TEXT", "TIMESTAMP", "DATE") {
		item[field] = string(bytesData)
	} else if str.Contains(typeName, "DECIMAL", "FLOAT") {
		f, _ := strconv.ParseFloat(string(bytesData), 64)
		item[field] = f
	} else {
		item[field] = bytesData
	}
}
