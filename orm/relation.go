package orm

import "github.com/enorith/database"

type Relation interface {
	RelateFrom() interface{}
	RelateTo() interface{}
	MarshalJSON() ([]byte, error)
	SetData(collection *database.Collection)
	Load() (*database.Collection, error)
}
