package rithythm

import (
	"github.com/CaoJiayuan/database"
)

type ModelHolder struct {
	model DataModel
}

func (h *ModelHolder) Query() *RithythmBuilder {
	b := new(RithythmBuilder)
	name := h.model.GetConnectionName()
	if len(name) < 1 {
		name = config.Default
	}
	connection := database.NewConnection(name, config)
	b.QueryBuilder = database.NewBuilder(connection)
	b.From(h.model.GetTable())
	return b.SetModel(h.model)
}

func (h *ModelHolder) With(loads ...string) *RithythmBuilder {
	return h.Query().With(loads...)
}

func (h *ModelHolder) Find(id int64, columns ...string) DataModel {
	m, err := h.Query().Find(id, columns...)
	if err != nil || m == nil {
		return &Model{
			valid: false,
		}
	}

	return m
}

func (h *ModelHolder) Make(data map[string]interface{}) DataModel {

	h.model.Unmarshal(database.NewCollectionItem(data))
	return h.model
}

func (h *ModelHolder) Create(data map[string]interface{}) (DataModel, error) {
	item, err := h.Query().Create(data, h.model.GetKeyName())

	if err != nil {
		return &Model{}, err
	}

	return ItemToModel(h.model, item), nil
}
