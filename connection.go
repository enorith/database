package rithdb

import (
	"database/sql"
	"errors"
	"fmt"
	env "github.com/CaoJiayuan/rithenv"
	ev "github.com/CaoJiayuan/rithev"
	"strings"
	"sync"
	"time"
)

type DriverRegister = func(config ConnectionConfig) (*sql.DB, error)

var driverRegister map[string]DriverRegister

var Conns Connections

var openDBs *OpenDBs

type OpenDBs struct {
	opened map[string]*sql.DB
	m      sync.RWMutex
}

func (d *OpenDBs) Get(name string) (*sql.DB, bool) {
	d.m.RLock()
	opened, exists := d.opened[name]
	d.m.RUnlock()
	return opened, exists
}

func (d *OpenDBs) Put(name string, db *sql.DB) {
	d.m.Lock()
	d.opened[name] = db
	d.m.Unlock()
}

type DBEvent struct {
	ev.Event
	Sql         string
	Type        string
	Bindings    []interface{}
	Err         error
	Millisecond time.Duration
}

func (e *DBEvent) GetEventName() string {
	return "rith::db"
}

func (e *DBEvent) GetRawSql() string {
	sqlStr := e.Sql
	for _, v := range e.Bindings {
		var (
			str    string
			format string
		)
		switch v.(type) {
		case int:
			format = "%d"
			break
		case int64:
			format = "%d"
			break
		case uint64:
			format = "%d"
			break
		case string:
			format = "'%s'"
			break
		}
		str = fmt.Sprintf(format, v)
		sqlStr = strings.Replace(sqlStr, "?", str, 1)
	}

	return sqlStr
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

func (c *Connection) Configure(cfg Config) *Connection {
	c.config = cfg

	return c
}

func (c *Connection) Clone() *Connection {
	return NewConnection(c.connection, c.config)
}

func (c *Connection) Select(sql string, bindings ...interface{}) (*sql.Rows, error) {
	db, err := c.GetDB()
	if err != nil {
		return nil, err
	}

	startAt := time.Now().Nanosecond()
	rows, queryErr := db.Query(sql, bindings...)
	millisecond := time.Duration(time.Now().Nanosecond()-startAt) / time.Millisecond
	ev.BUS.Dispatch(&DBEvent{
		Sql:         sql,
		Type:        "select",
		Err:         queryErr,
		Bindings:    bindings,
		Millisecond: millisecond,
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
		Sql:      sql,
		Type:     "exec",
		Err:      queryErr,
		Bindings: bindings,
	})

	return result, queryErr
}

func (c *Connection) InsertGetId(sql string, bindings ...interface{}) (int64, error) {
	result, execErr := c.Exec(sql, bindings...)
	if execErr != nil {
		return 0, execErr
	}

	id, err := result.LastInsertId()

	return id, err
}

func (c *Connection) TransactionCall(handler func() error) error {
	var err error

	db, dbErr := c.GetDB()
	if dbErr != nil {
		err = dbErr
	} else {
		tx, txErr := db.Begin()
		if txErr != nil {
			err = txErr
		} else {
			err = c.callTxHandler(handler)
			tx.Commit()
			if err != nil {
				tx.Rollback()
			}
		}
	}

	return err
}

func (c *Connection) callTxHandler(handler func() error) error {
	var err error
	defer func() {
		if x := recover(); x != nil {
			if e, ok := x.(error); ok {
				err = e
			}

			if s, ok := x.(string); ok {
				err = errors.New(s)
			}
		}
	}()
	err = handler()

	return err
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

	// if using opened connection
	if opened, exits := openDBs.Get(using); exits {
		c.db = opened
		return opened, nil
	}

	config := c.resolveConnectionConfig()

	c.setGrammar(config.Driver)
	register := driverRegister[config.Driver]
	db, err := register(config)
	//opened[using] = db
	openDBs.Put(using, db)
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

func init() {
	openDBs = &OpenDBs{map[string]*sql.DB{}, sync.RWMutex{}}
}
