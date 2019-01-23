package rithythm

import (
	"encoding/json"
	"github.com/CaoJiayuan/goutilities/define"
)

type DataModel interface {
	GetTable() string
	GetConnectionName() string
	GetKeyName() string
	marshal(data define.Map)
	Clone() DataModel
    GetValue(field string) interface{}
}

type Model struct {
	originals define.Map
}

func (m *Model) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.originals)
}

func (m *Model) GetTable() string {
	panic("GetTable: not implemented")
}

func (m *Model) Clone() DataModel {
	panic("Clone: not implemented")
}

func (m *Model) GetConnectionName() string {
	return ""
}

func (m *Model) GetKeyName() string {
	return "id"
}

func (m *Model) GetValue(field string) interface{} {
	return m.originals[field]
}

func (m *Model) marshal(data define.Map) {
	m.originals = data
}

func Hold(m DataModel) *ModelHolder {
	return &ModelHolder{m}
}