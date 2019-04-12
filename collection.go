package rithdb

import (
	"database/sql"
	"encoding/json"
	"github.com/CaoJiayuan/goutilities/str"
	"strconv"
	"errors"
	"fmt"
)

type Item interface{}
type ItemResolver func(item Item, key int) interface{}
type ItemFilter func(item Item, key int) bool

type CollectionItem struct {
	item Item
	itemMap map[string]interface{}
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

func (i *CollectionItem) Original() Item {
	return i.item
}

func (i *CollectionItem) IsNil() bool {
	return i.item == nil
}

func (i *CollectionItem) IsNotNil() bool {
	return !i.IsNil()
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
	m, err := i.ToMap()

	if err != nil {
		return nil, err
	}

	if v, exists := m[key]; exists {
		return v, nil
	}

	return nil, errors.New(fmt.Sprintf("map key [%s] not exists", key))
}


func (i *CollectionItem) ToMap() (map[string]interface{}, error) {
	if i.itemMap != nil {
		return i.itemMap, nil
	}
	if m,ok := i.item.(map[string]interface{});ok {
		return m, nil
	}
	return  nil, errors.New("collection item can not covert to map")
}


type Collection struct {
	items []Item
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

func (c *Collection) GetItem(key int) *CollectionItem {
	if len(c.items) < 1 {
		return NewCollectionItem(nil)
	}

	return NewCollectionItem(c.items[key])
}

func (c *Collection) First() *CollectionItem {
	return c.GetItem(0)
}

func (c *Collection) Map(re ItemResolver) *Collection {
	var result []Item
	for k, v := range c.items {
		result = append(result, re(v, k))
	}

	return Collect(result)
}

func (c *Collection) Filter(filter ItemFilter) *Collection {
	var result []Item
	for k, v := range c.items {
		if filter(v, k) {
			result = append(result, v)
		}
	}

	return Collect(result)
}

func (c *Collection) Pluck(value string) *Collection {
	var result []Item
	for _, v := range c.items {
		if t, o := v.(map[string]interface{}); o {
			if val, ok := t[value]; ok {
				result = append(result, val)
			}
		}
	}

	return Collect(result)
}

func (c *Collection) GetItems() []Item {
	return c.items
}

func Collect(items interface{}) *Collection {
	return &Collection{convertItems(items)}
}

func NewCollectionItem(item Item) *CollectionItem {
	return &CollectionItem{item: item}
}

func convertItems(items interface{}) []Item {
	if t, ok := items.(*sql.Rows); ok {
		defer t.Close()
		cols, _ := t.Columns()
		types, _ := t.ColumnTypes()
		var data []Item
		item := make([]interface{}, len(cols))
		values := make([][]byte, len(cols))

		for k := range values {
			item[k] = &values[k]
		}
		for t.Next() {
			t.Scan(item...)
			d := make(map[string]interface{})

			for k, v := range cols {
				columnType := types[k].DatabaseTypeName()

				bytesData := values[k]
				if str.Contains(columnType, "INT") {
					integer, _ := strconv.Atoi(string(bytesData))
					d[v] = integer
				} else if str.Contains(columnType, "CHAR", "TEXT", "TIMESTAMP", "DATE") {
					d[v] = string(bytesData)
				} else if str.Contains(columnType, "DECIMAL", "FLOAT") {
					f, _ := strconv.ParseFloat(string(bytesData), 64)
					d[v] = f
				} else {
					d[v] = bytesData
				}
			}
			data = append(data, d)
		}

		return data
	}
	if t, ok := items.([]Item); ok {
		return t
	}
	panic("invalid collection item gives")
}
