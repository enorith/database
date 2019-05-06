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

func (r *RithythmBuilder) Where(column string, operator string, value interface{}, and bool) *RithythmBuilder {
	r.QueryBuilder.Where(column, operator, value, and)
	return r
}

func (r *RithythmBuilder) WhereNull(column string, and bool) *RithythmBuilder {
	r.QueryBuilder.WhereNull(column, and)
	return r
}

func (r *RithythmBuilder) WhereNotNull(column string, and bool) *RithythmBuilder {
	r.QueryBuilder.WhereNotNull(column, and)
	return r
}

func (r *RithythmBuilder) WhereNest(and bool, handler rithdb.WhereNestHandler) *RithythmBuilder {
	r.QueryBuilder.WhereNest(and, handler)
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

func (r *RithythmBuilder) Find(id int64, columns... string) DataModel {
	first := r.Where(r.model.GetKeyName(), "=", id, true).Get(columns...).First()
	return ItemToModel(r.model, first)
}

func Config(c rithdb.Config)  {
	config = c
}