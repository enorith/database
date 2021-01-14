package database

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	ev "github.com/enorith/event"
)

const DefaultTimeout = 5 * time.Second

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

func (d *OpenDBs) Remove(name string) {
	d.m.Lock()
	delete(d.opened, name)
	d.m.Unlock()
}

type DBEvent struct {
	ev.Event
	Sql         string
	Type        string
	Bindings    []interface{}
	Err         error
	Microsecond time.Duration
}

func (e *DBEvent) GetEventName() string {
	return "enorith::db"
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
	GetDriver() string
}

type Connection struct {
	db      *sql.DB
	driver  string
	grammar Grammar
	dsn     string
	timeout time.Duration
}

func (c *Connection) GetDriver() string {
	return c.driver
}

func (c *Connection) dbKey() string {
	return c.driver + c.dsn
}

func (c *Connection) Close() error {
	if c.db != nil {
		e := c.db.Close()
		if e == nil {
			c.db = nil
			openDBs.Remove(c.dbKey())
		}
		return e
	}

	return nil
}

func (c *Connection) Clone() *Connection {
	return NewConnection(c.driver, c.dsn)
}

func (c *Connection) Select(sql string, bindings ...interface{}) (*sql.Rows, error) {
	db, err := c.GetDB()
	if err != nil {
		return nil, err
	}

	startAt := time.Now().Nanosecond()
	rows, queryErr := db.Query(sql, bindings...)
	microsecond := time.Duration(time.Now().Nanosecond()-startAt) / time.Microsecond

	ev.BUS.Dispatch(&DBEvent{
		Sql:         sql,
		Type:        "select",
		Err:         queryErr,
		Bindings:    bindings,
		Microsecond: microsecond,
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

func (c *Connection) GetDB() (*sql.DB, error) {
	key := c.dbKey()

	// if using opened connection
	if opened, exits := openDBs.Get(key); exits {
		c.db = opened
		return opened, nil
	}

	db, err := sql.Open(c.driver, c.dsn)
	if err != nil {
		return nil, err
	}

	openDBs.Put(key, db)
	c.db = db
	return db, err
}

func (c *Connection) GetGrammar() (Grammar, error) {
	if c.grammar != nil {
		return c.grammar, nil
	}
	grammar, ge := grammars[c.driver]
	if !ge {
		return nil, fmt.Errorf("grammar [%s] is not registed", c.driver)
	}

	c.grammar = grammar
	return grammar, nil
}

func (c *Connection) GetTimeout() time.Duration {
	if c.timeout == 0 {
		return DefaultTimeout
	}

	return c.timeout
}

func (c *Connection) Timeout(t time.Duration) *Connection {
	c.timeout = t
	return c
}

func NewConnection(driver, dsn string) *Connection {
	return &Connection{
		driver: driver,
		dsn:    dsn,
	}
}

func init() {
	openDBs = &OpenDBs{map[string]*sql.DB{}, sync.RWMutex{}}
}
