package rithythm

import (
	"github.com/CaoJiayuan/goutilities/define"
	"github.com/CaoJiayuan/rithdb"
)

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
	m := c.Collection.GetItem(key).(define.Map)
	item := c.model.Clone()
	item.marshal(m)
	return item
}