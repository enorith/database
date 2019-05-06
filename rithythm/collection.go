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
	return &RithythmCollection{
		c,
		model,
	}
}

func (c *RithythmCollection) GetItem(key int) DataModel {
	return ItemToModel(c.model, c.Collection.GetItem(key))
}

func (c *RithythmCollection) Map(re ModelResolver) *rithdb.Collection {
	var result []rithdb.Item
	for k, v := range c.GetItems() {
		result = append(result, re(ItemToModel(c.model, rithdb.NewCollectionItem(v)), k))
	}

	return rithdb.Collect(result)
}

func (c *RithythmCollection) Filter(re ModelFilter) *RithythmCollection {
	var result []rithdb.Item
	for k, v := range c.GetItems() {
		if re(ItemToModel(c.model, rithdb.NewCollectionItem(v)), k) {
			result = append(result, v)
		}
	}

	return CollectFromBase(rithdb.Collect(result), c.model)
}

func ItemToModel(model DataModel, item *rithdb.CollectionItem) DataModel {
	if item == nil {
		return nil
	}
	if data, ok := item.Original().(map[string]interface{}); ok {
		m := model.Clone()
		m.unmarshal(data)
		return m
	}

	return nil
}