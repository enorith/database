package rithdb

import "encoding/json"

type Paginator struct {
	builder *QueryBuilder
	page    int
	perPage int
	total   int64
}

func (p *Paginator) MarshalJSON() ([]byte, error) {
	result, _ := p.builder.ForPage(p.page, p.perPage).Get()
	data := map[string]interface{}{
		"total": p.GetTotal(),
		"page" : p.page,
		"page_size" : p.perPage,
		"data" : result,
	}

	return json.Marshal(data)
}

func (p *Paginator) GetTotal() int64 {
	if p.total < 0 {
		p.total = p.builder.CountForPage()
	}

	return p.total
}