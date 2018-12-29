package rithdb

var garmmars map[string]Grammar

type Grammar interface {
	compile(s QueryBuilder) string
}

// SqlGrammar is sql compiler
// compile QueryBuilder to sql string
type SqlGrammar struct {

}

func (g *SqlGrammar) compile(s QueryBuilder) string {
	panic("implement me")
}

type MysqlGrammar struct {
	SqlGrammar
}

func RegisterGarmmar(name string, g Grammar) {
	if garmmars == nil {
		 garmmars = make(map[string]Grammar)
	}
	garmmars[name] = g
}
