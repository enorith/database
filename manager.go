package rithdb

import (
	"database/sql"
	"fmt"
)

func WithDefaultDrivers() {
	registerDefaultDrivers()
	registerDefaultGrammars()
}

func registerDefaultDrivers() {
	AddDriverRegister("mysql", func(config ConnectionConfig) (*sql.DB, error) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.Username, config.Password, config.Host, config.Port, config.Database)
		return sql.Open("mysql", dsn)
	})
	AddDriverRegister("sqlite", func(config ConnectionConfig) (*sql.DB, error) {
		dsn := config.Database
		return sql.Open("sqlite3", dsn)
	})
}

func registerDefaultGrammars() {
	RegisterGrammar("mysql", &MysqlGrammar{})
}
