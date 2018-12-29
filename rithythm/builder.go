package rithythm

import (
	"github.com/CaoJiayuan/rithdb"
)

type ModelResolver func(model DataModel) interface{}

var config rithdb.Config

type RithythmBuilder struct {
	*rithdb.QueryBuilder
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

func Config(c rithdb.Config)  {
	config = c
}