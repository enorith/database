package database

import (
	"fmt"
	"github.com/enorith/cache"
	jsoniter "github.com/json-iterator/go"
	"sync"
)
var json = jsoniter.ConfigCompatibleWithStandardLibrary

type ConnectionRegister func() (*Connection, error)

var Cache cache.Repository

var DefaultConnection = "default"

var DefaultManager *Manager

type Manager struct {
	connectionName string
	registers 	   map[string]ConnectionRegister
	connections    map[string]*Connection
	m sync.RWMutex
}

func (m *Manager) Using(name string) *Manager {
	if m.connectionName != name {
		m.connectionName = name
	}

	return m
}

func (m *Manager) Register(name string, register ConnectionRegister) *Manager {
	m.registers[name] = register
	return m
}


func (m *Manager) GetConnection(name ...string) (*Connection, error){
	var using string
	if len(name) > 0 {
		using = name[0]
	} else {
		using = m.connectionName
	}
	m.Using(using)

	if len(m.connectionName) < 1 {
		m.connectionName = DefaultConnection
	}

	m.m.RLock()
	c, has := m.connections[m.connectionName]
	m.m.RUnlock()
	if has {
		return c, nil
	}
	register, exists := m.registers[m.connectionName]
	if  !exists {
		return nil, fmt.Errorf("unregisterd connection [%s]", m.connectionName)
	}
	c, e := register()
	if e != nil {
		return nil, fmt.Errorf("register connection error: %v", e)
	}

	m.setConnection(m.connectionName, c)

	return c, nil
}

func (m *Manager) CloseAll() error {
	m.m.Lock()
	for name, connection := range m.connections {
		e := connection.Close()
		if e != nil {
			return e
		}
		delete(m.connections, name)
	}
	m.m.Unlock()
	return nil
}

func (m *Manager) NewBuilder(connectionName ...string) (*QueryBuilder, error) {
	c, e := m.GetConnection(connectionName...)
	if e !=nil {
		return nil, e
	}

	return NewBuilder(c), nil
}

func (m *Manager) setConnection(name string, connection *Connection) {
	m.m.Lock()
	m.connections[name] = connection
	m.m.Unlock()
}

func WithDefaultDrivers() {
	WithMysql()
}

func WithMysql() {
	RegisterGrammar("mysql", &MysqlGrammar{})
}

func init() {
	DefaultManager = new(Manager)
	DefaultManager.registers = make(map[string]ConnectionRegister)
	DefaultManager.connections = make(map[string]*Connection)
	DefaultManager.m = sync.RWMutex{}
}