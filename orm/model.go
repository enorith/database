package orm

import (
	"github.com/enorith/database"
)

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
	m.valid = true
	m.v = v
	b := new(Builder)

	conn, err := guessConnection(v)
	if err != nil {
		return err
	}

	if len(conn) < 1 {
		conn = database.DefaultConnection
	}

	builder, be := database.DefaultManager.NewBuilder(conn)
	if be != nil {
		return be
	}
	b.QueryBuilder = builder
	m.Builder = b
	return nil
}


func NewModel(v interface{}) (*Model, error) {
	m := &Model{}
	e := m.InitWith(v)
	if e != nil {
		return nil, e
	}
	return m, nil
}