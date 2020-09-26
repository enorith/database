package database

import (
	"encoding/json"
	"math"
)

type lengthAwareMeta struct {
	Total       int64   `json:"total"`
	CurrentPage int     `json:"current_page"`
	LastPage    float64 `json:"last_page"`
	PerPage     int     `json:"per_page"`
	From        int64   `json:"from"`
	To          int64   `json:"to"`
}

type LengthAwareResult struct {
	Data  *Collection     `json:"data"`
	Meta  lengthAwareMeta `json:"meta"`
	Valid bool            `json:"-"`
}

type LengthAwarePaginator struct {
	builder *QueryBuilder
	page    int
	perPage int
	total   int64
	items   *Collection
}

func (p *LengthAwarePaginator) MarshalJSON() ([]byte, error) {
	result, err := p.ToResult()

	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (p *LengthAwarePaginator) GetTotal() int64 {
	if p.total < 0 {
		p.total = p.builder.CountForPage()
	}

	return p.total
}

func (p *LengthAwarePaginator) ToResult() (LengthAwareResult, error) {
	result, err := p.Items()

	if err != nil {
		return LengthAwareResult{}, err
	}

	total := p.GetTotal()

	lastPage := math.Ceil(float64(total) / float64(p.perPage))
	meta := lengthAwareMeta{
		Total:       total,
		CurrentPage: p.page,
		LastPage:    lastPage,
		PerPage:     p.perPage,
		From:        p.firstIndex(),
		To:          p.lastIndex(),
	}
	data := LengthAwareResult{
		Data:  result,
		Meta:  meta,
		Valid: true,
	}

	return data, nil
}

func (p *LengthAwarePaginator) firstIndex() int64 {
	return int64((p.page-1)*p.perPage + 1)
}

func (p *LengthAwarePaginator) Items() (*Collection, error) {
	if p.items != nil {
		return p.items, nil
	}
	var err error
	p.items, err = p.builder.ForPage(p.page, p.perPage).Get()
	return p.items, err
}

func (p *LengthAwarePaginator) lastIndex() int64 {
	items, err := p.Items()
	if err != nil {
		return 0
	}

	return p.firstIndex() + int64(items.Len()-1)
}
