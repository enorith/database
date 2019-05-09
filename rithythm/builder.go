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
func (r *RithythmBuilder) Get(columns... string) (*RithythmCollection, error) {
	c, err := r.QueryBuilder.Get(columns...)

	if err != nil {
		return nil, err
	}
	return CollectFromBase(c, r.model), err
}

func (r *RithythmBuilder) GetRaw(query string, bindings... interface{}) (*RithythmCollection, error) {
	c, err := r.QueryBuilder.GetRaw(query, bindings...)

	if err != nil {
		return nil, err
	}
	return CollectFromBase(c, r.model), nil
}

func (r *RithythmBuilder) First(columns... string) (DataModel, error) {
	c, err := r.Take(1).Get(columns...)

	if err != nil {
		return nil, err
	}

	first := c.First()
	return ItemToModel(r.model, first), nil
}

func (r *RithythmBuilder) Find(id int64, columns... string) (DataModel, error) {
	first, err := r.Where(r.model.GetKeyName(), "=", id, true).First(columns...)

	if err != nil {
		return  nil, err
	}

	return first, nil
}

func Config(c rithdb.Config)  {
	config = c
}