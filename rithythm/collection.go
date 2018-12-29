package rithythm

import (
	"github.com/CaoJiayuan/rith/database"
	"github.com/CaoJiayuan/goutilities/define"
)

type RithythmCollection struct {
	*database.Collection
	model DataModel
}

func CollectFromBase(c *database.Collection, model DataModel) *RithythmCollection {
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