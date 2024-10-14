package search

import (
	"errors"

	"github.com/mailstepcz/serr"
)

// ExprFlavour is a flavour of expressions.
type ExprFlavour int

// flavours
const (
	DocDB ExprFlavour = iota
	OpenSearch
)

func (fl ExprFlavour) String() string {
	switch fl {
	case DocDB:
		return "DocDB"
	case OpenSearch:
		return "OpenSearch"
	default:
		return "unknown flavour"
	}
}

// Expr is an AST node representing an expression.
type Expr interface {
	Idents() []string
	Map(ExprFlavour) (interface{}, error)
}

// Eq is an AST node for equality.
type Eq[T any] struct {
	Ident string
	Value T
}

// Idents returns all the identifiers in the expression.
func (e Eq[T]) Idents() []string {
	return []string{e.Ident}
}

// Map returns the query map corresponding to the expression.
func (e Eq[T]) Map(fl ExprFlavour) (interface{}, error) {
	switch fl {
	case DocDB:
		return Map{Pairs: []KVPair{
			{e.Ident, e.Value},
		}}, nil
	case OpenSearch:
		return Map{Pairs: []KVPair{
			{"term", Map{Pairs: []KVPair{
				{e.Ident, e.Value},
			}}}}}, nil
	}
	panic("unknown expression flavour: " + fl.String())
}

// Match is an AST node for fulltext matching.
type Match struct {
	Ident string
	Value string
}

// Idents returns all the identifiers in the expression.
func (e Match) Idents() []string {
	return []string{e.Ident}
}

// Map returns the query map corresponding to the expression.
func (e Match) Map(fl ExprFlavour) (interface{}, error) {
	switch fl {
	case DocDB:
		return nil, errors.ErrUnsupported
	case OpenSearch:
		return Map{Pairs: []KVPair{
			{"match", Map{Pairs: []KVPair{
				{e.Ident, e.Value},
			}}}}}, nil
	}
	panic("unknown expression flavour: " + fl.String())
}

// Interval is an AST node for ranges.
type Interval[T any] struct {
	Ident string
	From  *T
	To    *T
}

// Idents returns all the identifiers in the expression.
func (e Interval[T]) Idents() []string {
	return []string{e.Ident}
}

// Map returns the query map corresponding to the expression.
func (e Interval[T]) Map(fl ExprFlavour) (interface{}, error) {
	switch fl {
	case DocDB:
		conds := make([]KVPair, 0, 2)
		if e.From != nil {
			conds = append(conds, KVPair{"$gte", *e.From})
		}
		if e.To != nil {
			conds = append(conds, KVPair{"$lte", *e.To})
		}
		return Map{Pairs: []KVPair{
			{e.Ident, Map{Pairs: conds}}}}, nil
	case OpenSearch:
		conds := make([]KVPair, 0, 2)
		if e.From != nil {
			conds = append(conds, KVPair{"gte", *e.From})
		}
		if e.To != nil {
			conds = append(conds, KVPair{"lte", *e.To})
		}
		return Map{Pairs: []KVPair{
			{"range", Map{Pairs: []KVPair{
				{e.Ident, Map{Pairs: conds}}}}}}}, nil
	}
	panic("unknown expression flavour: " + fl.String())
}

// And is an AST node for conjunction.
type And struct {
	Exprs []Expr
}

// Idents returns all the identifiers in the expression.
func (e And) Idents() []string {
	var idents []string
	for _, el := range e.Exprs {
		idents = append(idents, el.Idents()...)
	}
	return idents
}

// Map returns the query map corresponding to the expression.
func (e And) Map(fl ExprFlavour) (interface{}, error) {
	switch fl {
	case DocDB:
		m := Map{Pairs: make([]KVPair, 0, len(e.Exprs))}
		for _, e := range e.Exprs {
			em, err := e.Map(fl)
			if err != nil {
				return nil, err
			}
			if em2, ok := em.(Map); ok {
				m.Pairs = append(m.Pairs, em2.Pairs...)
			} else {
				return nil, serr.New("expected map", serr.Any("expr", em))
			}
		}
		return m, nil
	case OpenSearch:
		l := make([]Map, 0, len(e.Exprs))
		for _, e := range e.Exprs {
			em, err := e.Map(fl)
			if err != nil {
				return nil, err
			}
			if em2, ok := em.(Map); ok {
				l = append(l, em2)
			} else {
				return nil, serr.New("expected map", serr.Any("expr", em))
			}
		}
		return l, nil
	}
	panic("unknown expression flavour: " + fl.String())
}
