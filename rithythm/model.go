package rithythm

import (
	"github.com/CaoJiayuan/rithdb"
)

type RelationModel interface {
	DataModel
	Relations() map[string]Relation
}

type DataModel interface {
	GetTable() string
	GetConnectionName() string
	GetKeyName() string
	Unmarshal(data *rithdb.CollectionItem)
	Clone() DataModel
	GetValue(field string) interface{}
	GetString(field string) (string, error)
	GetInt(field string) (int64, error)
	GetUint(field string) (uint64, error)
	MarshalJSON() ([]byte, error)
	Original() map[string]interface{}
	IsValid() bool
	MarshalToCache() interface{}
	UnmarshalFromCache(decoder func(value interface{}) bool) bool
	Query() *RithythmBuilder
}

type Model struct {
	item  *rithdb.CollectionItem
	valid bool
}

func (m *Model) MarshalToCache() interface{} {
	return m.Original()
}

func (m *Model) UnmarshalFromCache(decoder func(value interface{}) bool) bool {
	var data map[string]interface{}
	m.valid = decoder(&data)
	m.item = rithdb.NewCollectionItem(data)
	return m.valid
}

func (m *Model) MarshalJSON() ([]byte, error) {
	return m.item.MarshalJSON()
}

func (m *Model) Original() map[string]interface{} {
	return m.item.Original()
}

func (m *Model) GetTable() string {
	panic("GetTable: not implemented")
}

func (m *Model) Clone() DataModel {
	panic("Clone: not implemented")
}

func (m *Model) IsValid() bool {
	return m.valid
}

func (m *Model) GetConnectionName() string {
	return ""
}

func (m *Model) GetKeyName() string {
	return "id"
}

func (m *Model) GetValue(field string) interface{} {
	v, err := m.item.GetValue(field)
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

func (m *Model) GetUint(field string) (uint64, error) {
	return m.item.GetUint(field)
}

func (m *Model) Unmarshal(data *rithdb.CollectionItem) {
	m.item = data
	m.valid = true
}

func (m *Model) Query() *RithythmBuilder {
	return Hold(m).Query()
}

func Hold(m DataModel) *ModelHolder {
	return &ModelHolder{m}
}
