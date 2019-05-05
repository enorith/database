package rithythm

import (
	"encoding/json"
	"github.com/CaoJiayuan/goutilities/define"
	"errors"
	"fmt"
)

type DataModel interface {
	GetTable() string
	GetConnectionName() string
	GetKeyName() string
	marshal(data define.Map)
	Clone() DataModel
    GetValue(field string) interface{}
	GetString(field string) (string, error)
	GetInt(field string) (int, error)
	GetInt64(field string) (int64, error)
	GetInt32(field string) (int32, error)
	MarshalJSON() ([]byte, error)
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

func (m *Model) GetString(field string) (string, error) {
	if s, ok := m.GetValue(field).(string); ok {
		return s, nil
	}

	return "", errors.New(fmt.Sprintf("try to get string value from field [%s]", field))
}

func (m *Model) GetInt(field string) (int, error) {
	if i, ok := m.GetValue(field).(int); ok {
		return i, nil
	}

	return 0, errors.New(fmt.Sprintf("try to get int value from field [%s]", field))
}

func (m *Model) GetInt64(field string) (int64, error) {
	if i, ok := m.GetValue(field).(int64); ok {
		return i, nil
	}

	return 0, errors.New(fmt.Sprintf("try to get int64 value from field [%s]", field))
}

func (m *Model) GetInt32(field string) (int32, error) {
	if i, ok := m.GetValue(field).(int32); ok {
		return i, nil
	}

	return 0, errors.New(fmt.Sprintf("try to get int32 value from field [%s]", field))
}

func (m *Model) marshal(data define.Map) {
	m.originals = data
}

func Hold(m DataModel) *ModelHolder {
	return &ModelHolder{m}
}