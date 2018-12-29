package rithythm

import "github.com/CaoJiayuan/rith/database"

type ModelResolver func(model DataModel) interface{}

type RithythmBuilder struct {
	*database.QueryBuilder
	model DataModel
}

func (r *RithythmBuilder) SetModel(model DataModel) *RithythmBuilder {
	r.model = model
	return r
}

func (r *RithythmBuilder) GetRaw(query string, bindings... interface{}) *RithythmCollection {
	c := r.QueryBuilder.GetRaw(query, bindings...)

	return CollectFromBase(c, r.model)
}