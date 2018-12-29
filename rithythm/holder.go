package rithythm

import "github.com/CaoJiayuan/rith/database"

type ModelHolder struct {
	model DataModel
}

func (h *ModelHolder) Query() *RithythmBuilder {
	b := new(RithythmBuilder)
	b.QueryBuilder = database.NewDefaultBuilder()
	b.From(h.model.GetTable())
	return b.SetModel(h.model)
}