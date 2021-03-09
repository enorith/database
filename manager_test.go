package database_test

import (
	"testing"

	"github.com/enorith/database"
)

var m *database.Manager

func Test_ManagerRegister(t *testing.T) {
	m.Register("default", func() (*database.Connection, error) {
		return database.NewConnection("mysql", "root:root@(127.0.0.1:3306)/labor"), nil
	})
	c, e := m.GetConnection("default")
	t.Logf("get connection %v", c)
	if e != nil {
		t.Fatalf("get connection error %v", e)
	}
}

func init() {
	database.WithDefaultDrivers()

	m = database.NewManager()
}
