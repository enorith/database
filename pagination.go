package database

import (
	"encoding/json"
	"math"
)

type Paginator struct {
	builder *QueryBuilder
	page    int
	perPage int
	total   int64
	items   *Collection
}

func (p *Paginator) MarshalJSON() ([]byte, error) {
	result, err := p.ToResult()

	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (p *Paginator) GetTotal() int64 {
	if p.total < 0 {
		p.total = p.builder.CountForPage()
	}

	return p.total
}

func (p *Paginator) ToResult() (map[string]interface{}, error) {
	result, err := p.Items()

	if err != nil {
		return nil, err
	}

	total := p.GetTotal()

	lastPage := math.Ceil(float64(total) / float64(p.perPage))
	data := map[string]interface{}{
		"total":        total,
		"current_page": p.page,
		"per_page":     p.perPage,
		"data":         result,
		"from":         p.firstIndex(),
		"to":           p.lastIndex(),
		"last_page":    lastPage,
	}

	return data, nil
}

func (p *Paginator) firstIndex() int64 {
	return int64((p.page-1)*p.perPage + 1)
}

func (p *Paginator) Items() (*Collection, error) {
	if p.items != nil {
		return p.items, nil
	}
	var err error
	p.items, err = p.builder.ForPage(p.page, p.perPage).Get()
	return p.items, err
}

func (p *Paginator) lastIndex() int64 {
	items, err := p.Items()
	if err != nil {
		return 0
	}

	return p.firstIndex() + int64(items.Len()-1)
}
