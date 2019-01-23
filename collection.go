package rithdb

import (
	"database/sql"
	"encoding/json"
	"github.com/CaoJiayuan/goutilities/str"
	"strconv"
)

type Item interface{}
type ItemResolver func(item Item, key int) interface{}
type ItemFilter func(item Item, key int) bool

type Collection struct {
	items []Item
}

func (c *Collection) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.items)
}

func (c *Collection) ToJson() []byte {
	j, err := c.MarshalJSON()

	if err != nil{
		panic(err)
	}

	return  j
}


func (c *Collection) GetItem(key int) Item {
	return c.items[key]
}


func (c *Collection) First() Item {
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
		if filter(v, k)  {
			result = append(result, v)
		}
	}

	return Collect(result)
}

func (c *Collection) Pluck(value string) *Collection {
	var result []Item
	for _, v := range c.items {
		if t,o := v.(map[string]interface{}) ; o {
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


func convertItems(items interface{}) []Item {
	if t, ok := items.(*sql.Rows); ok {
		defer t.Close()
		cols, _ := t.Columns()
		types, _ := t.ColumnTypes()
		data := []Item{}
		item := make([]interface{}, len(cols))
		values := make([][]byte, len(cols))

		for k := range values {
			item[k] = &values[k]
		}
		for t.Next() {
			t.Scan(item...)
			d := make(map[string]interface{})

			for k, v := range cols {
				t := types[k].DatabaseTypeName()

				bytesData := values[k]
				if str.Contains(t, "INT") {
					integer,_ := strconv.Atoi(string(bytesData))
					d[v] = integer
				} else if str.Contains(t, "CHAR", "TEXT", "TIMESTAMP") {
					d[v] = string(bytesData)
				} else if str.Contains(t, "DECIMAL", "FLOAT") {
					f,_ := strconv.ParseFloat(string(bytesData), 64)
					d[v] = f
				} else  {
					d[v] = bytesData
				}
			}
			data = append(data, d)
		}

		return  data
	}
	if t, ok := items.([]Item); ok {
		return t
	}
	panic("invalid collection item gives")
}