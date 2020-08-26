package rithythm

import (
	"github.com/CaoJiayuan/rithdb"
)

type ModelResolver func(model DataModel, key int) interface{}
type ModelFilter func(model DataModel, key int) bool

type RithythmCollection struct {
	*rithdb.Collection
	model DataModel
}

func CollectFromBase(c *rithdb.Collection, model DataModel) *RithythmCollection {
	collection := &RithythmCollection{}

	collection.Collection = c

	collection.model = model

	return collection
}

func NewCollection(model DataModel) *RithythmCollection {
	return CollectFromBase(&rithdb.Collection{}, model)
}

func (c *RithythmCollection) GetItem(key int) DataModel {
	return ItemToModel(c.model, c.Collection.GetItem(key))
}

func ItemToModel(model DataModel, item *rithdb.CollectionItem) DataModel {
	m := model.Clone()
	m.Unmarshal(item)

	return m
}
