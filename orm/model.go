package orm

type WithTable interface {
	Table() string
}

type WithKey interface {
	KeyName() string
}

type WithRelations interface {
	Relations() map[string]Relation
}

type WithConnection interface {
	Connection() string
}
