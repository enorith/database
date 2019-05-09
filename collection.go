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
)

type ItemResolver func(item CollectionItem, key int) interface{}
type ItemFilter func(item CollectionItem, key int) bool

type CollectionItem struct {
	item map[string]interface{}
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
	v, err :=  i.GetValue(key)
	if err != nil {
		return true
	}

	return v == nil
}

func (i *CollectionItem) IsNotNil(key string) bool {
	return !i.IsNil(key)
}

func (i *CollectionItem) GetInt(key string) (int, error) {
	v, err := i.GetValue(key)
	if err == nil {
		if i, ok := v.(int); ok {
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
	items []CollectionItem
	iterator *RowsIterator
	loaded bool
}

func (c *Collection) MarshalJSON() ([]byte, error) {
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

func (c *Collection) Each(resolver ItemResolver) {
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

func (i *RowsIterator) Read() map[string]interface{} {
	item := make([]interface{}, len(i.columns))
	values := make([][]byte, len(i.columns))
	for k := range values {
		item[k] = &values[k]
	}
	dataItem := make(map[string]interface{})
	i.rows.Scan(item...)

	for index, field := range i.columns {
		columnType := i.types[index].DatabaseTypeName()

		bytesData := values[index]
		parseType(dataItem, field, columnType, bytesData)
	}
	return dataItem
}

func CollectRows(rows *sql.Rows) (*Collection , error) {
	ite,err := NewRowsIterator(rows)
	if err != nil {
		return nil, err
	}

	return &Collection{
		iterator: ite,
	}, nil
}

func NewCollectionItem(item map[string]interface{}) *CollectionItem {
	return &CollectionItem{item: item}
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

func parseType(item map[string]interface{}, field string, columnType string, bytesData []byte) {
	if str.Contains(columnType, "INT") {
		integer, _ := strconv.Atoi(string(bytesData))
		item[field] = integer
	} else if str.Contains(columnType, "CHAR", "TEXT", "TIMESTAMP", "DATE") {
		item[field] = string(bytesData)
	} else if str.Contains(columnType, "DECIMAL", "FLOAT") {
		f, _ := strconv.ParseFloat(string(bytesData), 64)
		item[field] = f
	} else {
		item[field] = bytesData
	}
}
