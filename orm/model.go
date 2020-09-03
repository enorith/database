package orm

type TabledModel interface {
	GetTable() string
}

type KeyedModel interface {
	GetKeyName() string
}
