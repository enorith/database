package orm

import (
	"github.com/enorith/database"
)

var defaultConfig database.Config

type WithTable interface {
	Table() string
}

type WithKey interface {
	KeyName() string
}

type WithRelations interface {
	Relations() map[string]Relation
}

type WithConnection interface {
	Connection() string
}

type Model struct {
	*Builder
	valid      bool
	connection string
	table      string
	keyName    string
	v          interface{}
}

func (m *Model) InitWith(v interface{}) error {
	conn, err := guessConnection(v)
	if err != nil {
		return err
	}
	m.connection = conn
	table, e := guessTableName(v)
	if e != nil {
		return e
	}
	m.table = table

	key := guessKeyName(v)
	m.keyName = key
	m.valid = true
	m.v = v

	b := new(Builder)
	name := m.Connection()
	if len(name) < 1 {
		name = defaultConfig.Default
	}
	connection := database.NewConnection(name, defaultConfig)
	b.QueryBuilder = database.NewBuilder(connection)
	b.From(m.Table())
	m.Builder = b
	return nil
}

func (m *Model) Connection() string {
	return m.connection
}

func (m *Model) Table() string {
	return m.table
}

func (m *Model) KeyName() string {
	return m.keyName
}

func Config(c database.Config) {
	defaultConfig = c
}

func NewModel(v interface{}) (*Model, error) {
	m := &Model{}
	e := m.InitWith(v)
	if e != nil {
		return nil, e
	}
	return m, nil
}
