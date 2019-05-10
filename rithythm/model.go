package rithythm

import (
	"encoding/json"
	"github.com/CaoJiayuan/rithdb"
)

type DataModel interface {
	GetTable() string
	GetConnectionName() string
	GetKeyName() string
	unmarshal(data rithdb.CollectionItem)
	Clone() DataModel
    GetValue(field string) interface{}
	GetString(field string) (string, error)
	GetInt(field string) (int64, error)
	GetUInt(field string) (uint64, error)
	MarshalJSON() ([]byte, error)
	GetOriginals() map[string]interface{}
}

type Model struct {
	originals map[string]interface{}
	item rithdb.CollectionItem
}

func (m *Model) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.originals)
}

func (m *Model) GetOriginals() map[string]interface{} {
	return m.originals
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
	v,err := m.item.GetValue(field)
	if err != nil {
		return nil
	}

	return v
}

func (m *Model) GetString(field string) (string, error) {
	return m.item.GetString(field)
}

func (m *Model) GetInt(field string) (int64, error) {
	return m.item.GetInt(field)
}

func (m *Model) GetUInt(field string) (uint64, error) {
	return m.item.GetUInt(field)
}

func (m *Model) unmarshal(data rithdb.CollectionItem) {
	m.item = data
	m.originals = data.Original()
}

func Hold(m DataModel) *ModelHolder {
	return &ModelHolder{m}
}