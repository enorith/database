package database_test

import (
	"bytes"
	"github.com/enorith/database"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"testing"
)

var builder *database.QueryBuilder

func TestQueryBuilder_BasicQuery(t *testing.T) {
	sql, e := builder.From("user").Where("id", "=", 1, true).Get()

	if e != nil {
		t.Fatalf("query error %v", e)
	}

	t.Logf("find data count %d", sql.Len())
	t.Logf("find data %v", sql.GetItems())
}

func TestQueryBuilder_Create(t *testing.T) {
	item, e := builder.From("user").Create(map[string]interface{}{
		"name":  "jack",
		"email": "jack@gmail.com",
		"age":   29,
	})
	if e != nil {
		t.Fatalf("create data error %v", e)
	}
	if !item.IsValid() {
		t.Fatalf("create return item is invalid")
	}
	t.Logf("created item %v", item)
}

func init() {
	m = database.DefaultManager
	m.Register(database.DefaultConnection, func() (*database.Connection, error) {
		return database.NewConnection("mysql", "root:root@(127.0.0.1:13306)/test"), nil
	})

	builder, _ = m.NewBuilder()
	c, _ := m.GetConnection()
	sql, e := ioutil.ReadFile("./migration.sql")
	if e != nil {
		log.Fatalf("read migartion error %v", e)
	}

	tokens := bytes.Split(sql, []byte(";"))

	for _, token := range tokens {
		token = bytes.TrimSpace(token)
		if len(token) > 0 {
			_, err := c.Select(string(token))
			if err != nil {
				log.Fatalf("migartion error %v", err)
			}
		}
	}
}
