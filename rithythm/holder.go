package rithythm

import (
	"github.com/CaoJiayuan/rithdb"
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
	connection := rithdb.NewConnection(name, config)
	b.QueryBuilder = rithdb.NewBuilder(connection)
	b.From(h.model.GetTable())
	return b.SetModel(h.model)
}