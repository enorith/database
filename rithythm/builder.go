package rithythm

import (
	"github.com/CaoJiayuan/rithdb"
	"time"
)

var config rithdb.Config

type RithythmBuilder struct {
	*rithdb.QueryBuilder
	model DataModel
	loads []string
}

func (r *RithythmBuilder) With(loads ...string) *RithythmBuilder {
	r.loads = loads
	return r
}

func (r *RithythmBuilder) SetModel(model DataModel) *RithythmBuilder {
	r.model = model
	return r
}

func (r *RithythmBuilder) Select(columns ...string) *RithythmBuilder {
	r.QueryBuilder.Select(columns...)
	return r
}

func (r *RithythmBuilder) Where(column, operator string, value interface{}, and bool) *RithythmBuilder {
	r.QueryBuilder.Where(column, operator, value, and)
	return r
}

func (r *RithythmBuilder) WhereIn(column string, value []interface{}, and bool) *RithythmBuilder {
	r.QueryBuilder.WhereIn(column, value, and)
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

func (r *RithythmBuilder) WhereNest(and bool, handler rithdb.QueryHandler) *RithythmBuilder {
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

func (r *RithythmBuilder) Get(columns ...string) (*RithythmCollection, error) {
	c, err := r.QueryBuilder.Get(columns...)

	if err != nil {
		return nil, err
	}
	data := CollectFromBase(c, r.model)

	if m, ok := r.model.(RelationModel); ok {
		r.eagerLoad(data, m)
	}

	return data, nil
}

func (r *RithythmBuilder) eagerLoad(data *RithythmCollection, model RelationModel) {

	data.GetItems()
	relations := map[string]Relation{}
	for _, l := range r.loads {
		rs := model.Relations()
		if rel, ok := rs[l]; ok {
			rel.SetData(data)
			relations[l] = rel
		}
	}

	data.Each(func(item *rithdb.CollectionItem, index int) {
		for k, v := range relations {
			model := ItemToModel(model, item)
			if re, ok := v.(RelationOne); ok {
				data := v.RelateTo()
				re.MatchMarshal(model, data)
				item.Set(k, data)
			} else if re, ok := v.(RelationMany); ok {
				data := NewCollectionEmpty(v.RelateTo())
				re.MatchMarshal(model, data)
				item.Set(k, data)
			}
		}
	})
}

func (r *RithythmBuilder) GetRaw(query string, bindings ...interface{}) (*RithythmCollection, error) {
	c, err := r.QueryBuilder.GetRaw(query, bindings...)

	if err != nil {
		return nil, err
	}
	return CollectFromBase(c, r.model), nil
}

func (r *RithythmBuilder) First(columns ...string) (DataModel, error) {
	c, err := r.Take(1).Get(columns...)

	if err != nil {
		return nil, err
	}
	if c.Len() < 1 {
		return &Model{}, nil
	}

	first := c.First()
	return ItemToModel(r.model, first), nil
}

func (r *RithythmBuilder) Find(id int64, columns ...string) (DataModel, error) {
	first, err := r.Where(r.model.GetKeyName(), "=", id, true).First(columns...)

	if err != nil {
		return nil, err
	}

	return first, nil
}

func (r *RithythmBuilder) Remember(key string, d time.Duration) (*RithythmCollection, error) {
	col, err := r.QueryBuilder.Remember(key, d)

	return CollectFromBase(col, r.model), err
}

func Config(c rithdb.Config) {
	config = c
}
