package rithythm

import (
	"github.com/enorith/database"
)

type ModelResolver func(model DataModel, key int) interface{}
type ModelFilter func(model DataModel, key int) bool

type RithythmCollection struct {
	*database.Collection
	model DataModel
}

func CollectFromBase(c *database.Collection, model DataModel) *RithythmCollection {
	collection := &RithythmCollection{}

	collection.Collection = c

	collection.model = model

	return collection
}

func NewCollection(model DataModel) *RithythmCollection {
	return CollectFromBase(&database.Collection{}, model)
}

func (c *RithythmCollection) GetItem(key int) DataModel {
	return ItemToModel(c.model, c.Collection.GetItem(key))
}

func NewCollectionEmpty(model DataModel) *RithythmCollection {
	return CollectFromBase(database.NewCollectionEmpty(), model)
}

func ItemToModel(model DataModel, item *database.CollectionItem) DataModel {
	m := model.New()
	m.Unmarshal(item)

	return m
}
