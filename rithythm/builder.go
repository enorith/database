package rithythm

import (
	"github.com/CaoJiayuan/rithdb"
)


var config rithdb.Config

type RithythmBuilder struct {
	*rithdb.QueryBuilder
	model DataModel
}

func (r *RithythmBuilder) SetModel(model DataModel) *RithythmBuilder {
	r.model = model
	return r
}

func (r *RithythmBuilder) Select(columns... string) *RithythmBuilder {
	r.QueryBuilder.Select(columns...)
	return r
}

func (r *RithythmBuilder) Take(limit int) *RithythmBuilder {
	r.QueryBuilder.Take(limit)
	return r
}

func (r *RithythmBuilder) Offset(offset int) *RithythmBuilder {
	r.QueryBuilder.Offset(offset)
	return r
}

func (r *RithythmBuilder) ForPage(page int, perPage int) *RithythmBuilder {
	r.QueryBuilder.ForPage(page, perPage)
	return r
}
func (r *RithythmBuilder) Get(columns... string) *RithythmCollection {
	c := r.QueryBuilder.Get(columns...)
	return CollectFromBase(c, r.model)
}

func (r *RithythmBuilder) GetRaw(query string, bindings... interface{}) *RithythmCollection {
	c := r.QueryBuilder.GetRaw(query, bindings...)
	return CollectFromBase(c, r.model)
}

func (r *RithythmBuilder) First(columns... string) DataModel {
	first := r.Take(1).Get(columns...).First()
	return ItemToModel(r.model, first)
}

func Config(c rithdb.Config)  {
	config = c
}