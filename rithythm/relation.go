package rithythm

import (
	"fmt"

	"github.com/enorith/database"
	"github.com/jinzhu/inflection"
)

type RelationOne interface {
	MatchMarshal(d DataModel, out DataModel)
}

type RelationMany interface {
	MatchMarshal(d DataModel, out *RithythmCollection)
}

type Relation interface {
	RelateFrom() DataModel
	RelateTo() DataModel
	MarshalJSON() ([]byte, error)
	SetData(*RithythmCollection)
	Load() (*RithythmCollection, error)
}

type SimpleRelation struct {
	from DataModel
	to   DataModel
}

func (s *SimpleRelation) RelateFrom() DataModel {
	return s.from
}

func (s *SimpleRelation) RelateTo() DataModel {
	return s.to
}

type HasMany struct {
	*SimpleRelation
	foreignKey string
	localKey   string
	fromData   *RithythmCollection
	loaded     *RithythmCollection
}

func (h *HasMany) SetData(d *RithythmCollection) {
	h.fromData = d
}

func (h *HasMany) MarshalJSON() ([]byte, error) {
	col, err := h.Load()

	if err != nil {
		return nil, err
	}
	return col.MarshalJSON()
}

func (h *HasMany) Load() (*RithythmCollection, error) {
	if h.loaded != nil {
		return h.loaded, nil
	}
	var err error
	ids := h.fromData.Pluck(h.localKey)
	h.loaded, err = Hold(h.RelateTo()).Query().WhereIn(h.foreignKey, ids, true).Get()

	return h.loaded, err
}

func (h *HasMany) MatchMarshal(d DataModel, out *RithythmCollection) {
	col, err := h.Load()
	if err == nil && col != nil {
		col.GetItems()
		col.Each(func(item *database.CollectionItem, index int) {
			foreign, e := item.GetUint(h.foreignKey)
			local, e2 := d.GetUint(h.localKey)
			if e == nil && e2 == nil && local == foreign {
				out.Append(item)
			}
		})
	}
}

func NewHasMany(from, to DataModel, keys ...string) *HasMany {

	var f, l string
	switch len(keys) {
	case 0:
		f = fmt.Sprintf("%s_id", inflection.Singular(from.GetTable()))
		l = "id"
	case 1:
		f = keys[0]
		l = "id"
	case 2:
		f = keys[0]
		l = keys[1]
	default:
		f = keys[0]
		l = keys[1]
	}

	return &HasMany{SimpleRelation: &SimpleRelation{from: from, to: to}, foreignKey: f, localKey: l}
}
