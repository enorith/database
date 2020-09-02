package database

type Connections struct {
	items []*Connection
}

func (cs *Connections) Push(c *Connection) {
	cs.items = append(cs.items, c)
}

func (cs *Connections) Shift() (c *Connection) {
	if len(cs.items) < 1 {
		return nil
	}

	c, cs.items = cs.items[0], cs.items[1:]
	return
}

func (cs *Connections) ShiftAndClose() (c *Connection) {
	if c = cs.Shift(); c != nil {
		c.Close()
	}

	return
}

func (cs *Connections) Length() int {
	return len(cs.items)
}

func (cs *Connections) Empty() bool {
	return len(cs.items) == 0
}
