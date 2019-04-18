package rithdb

import (
	"database/sql"
	env "github.com/CaoJiayuan/rithenv"
	ev "github.com/CaoJiayuan/rithev"
)

type DriverRegister = func(config ConnectionConfig) (*sql.DB, error)

var driverRegister map[string]DriverRegister

var Conns Connections
var opened = map[string]*sql.DB{}

type DBEvent struct {
	ev.Event
	Sql string
	Type string
	Bindings []interface{}
	Err error
}

func (e *DBEvent) GetEventName() string {
	return "rith::db"
}

type ConnectionInterface interface {
	GetConnection() string
}

type Connection struct {
	db         *sql.DB
	connection string
	config     Config
	grammar    Grammar
}

func (c *Connection) GetConnection() string {
	return c.connection
}

func (c *Connection) Close() error {
	if c.db != nil {
		e := c.db.Close()
		if e == nil {
			c.db = nil
		}
		return e
	}

	return nil
}

func (c *Connection) Clone() *Connection {
	return NewConnection(c.connection, c.config)
}

func (c *Connection) Select(sql string, bindings ...interface{}) (*sql.Rows, error) {
	db, err := c.GetDB()
	if err != nil {
		return nil, err
	}

	rows, queryErr := db.Query(sql, bindings...)
	ev.BUS.Dispatch(&DBEvent{
		Sql: sql,
		Type:"select",
		Err: queryErr,
		Bindings: bindings,
	})

	return rows, queryErr
}

func (c *Connection) Exec(sql string, bindings ...interface{}) (sql.Result, error) {
	db, err := c.GetDB()
	if err != nil {
		return nil, err
	}

	result, queryErr := db.Exec(sql, bindings...)

	ev.BUS.Dispatch(&DBEvent{
		Sql: sql,
		Type: "exec",
		Err: queryErr,
		Bindings: bindings,
	})

	return result, queryErr
}

func (c *Connection) InsertGetId(sql string, bindings ...interface{}) (int64, error) {
	result, execErr := c.Exec(sql, bindings)
	if execErr != nil {
		return 0, execErr
	}

	id,err := result.LastInsertId()

	return id, err
}

// Using known connection
// well close current connection before use new connection
func (c *Connection) Using(connection string) *Connection {
	if connection != c.connection {
		c.Close()
		c.connection = connection
	}
	return c
}

func (c *Connection) GetDB(connection ...string) (*sql.DB, error) {
	var using string
	if len(connection) > 0 {
		using = connection[0]
	} else if len(c.connection) < 1 {
		using = c.config.Default
	} else {
		using = c.connection
	}

	if c.db != nil && using == c.connection {
		return c.db, nil
	}

	c.Using(using)

	if opened, exits := opened[using]; exits {
		c.db = opened
		return opened, nil
	}

	config := c.resolveConnectionConfig()

	c.setGrammar(config.Driver)
	register := driverRegister[config.Driver]
	db, err := register(config)
	opened[using] = db
	c.db = db
	return db, err
}
func (c *Connection) setGrammar(g string) {
	c.grammar = grammars[g]
}

func (c *Connection) resolveConnectionConfig() ConnectionConfig {
	conf := c.config.Connections[c.connection]

	if conf.Port == 0 {
		switch c.connection {
		case "mysql":
			conf.Port = 3306
		}
	}

	if len(conf.Database) == 0 {
		conf.Database = env.GetString("DB_DATABASE", "")
	}

	if len(conf.Username) == 0 {
		conf.Database = env.GetString("DB_USERNAME", "")
	}

	if len(conf.Password) == 0 {
		conf.Database = env.GetString("DB_PASSWORD", "")
	}

	if len(conf.Host) == 0 {
		conf.Database = env.GetString("DB_HOST", "127.0.0.1")
	}

	return conf
}

func AddDriverRegister(driver string, register DriverRegister) {
	if driverRegister == nil {
		driverRegister = map[string]DriverRegister{}
	}
	driverRegister[driver] = register
}

func NewConnection(conn string, config Config) *Connection {
	connection := &Connection{
		connection: conn,
		config:     config,
	}
	connections := config.Connections
	if config, exists := connections[conn]; exists {
		connection.setGrammar(config.Driver)
	}

	Conns.Push(connection)
	return connection
}
