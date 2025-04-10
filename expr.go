package search

import (
	"errors"
	"fmt"

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

// Wildcard is an AST node for wildcard term queries.
type Wildcard struct {
	Ident string
	Value string
}

// Idents returns all the identifiers in the expression.
func (e Wildcard) Idents() []string {
	return []string{e.Ident}
}

// Map returns the query map corresponding to the expression.
func (e Wildcard) Map(fl ExprFlavour) (interface{}, error) {
	switch fl {
	case DocDB:
		return nil, errors.ErrUnsupported
	case OpenSearch:

		return Map{[]KVPair{KVPair{
			"wildcard", Map{
				[]KVPair{
					{e.Ident, Map{[]KVPair{
						{"value", fmt.Sprintf("*%s*", e.Value)},
						{"case_insensitive", true},
					}}}}}}}}, nil
	}

	panic("unknown expression flavour: " + fl.String())
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
	Ident         string
	From          *T
	FromInclusive bool
	To            *T
	ToInclusive   bool
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
			op := "$gte"
			if !e.FromInclusive {
				op = "$gt"
			}
			conds = append(conds, KVPair{op, *e.From})
		}
		if e.To != nil {
			op := "$lte"
			if !e.ToInclusive {
				op = "$lt"
			}
			conds = append(conds, KVPair{op, *e.To})
		}
		return Map{Pairs: []KVPair{
			{e.Ident, Map{Pairs: conds}}}}, nil
	case OpenSearch:
		conds := make([]KVPair, 0, 2)
		if e.From != nil {
			op := "gte"
			if !e.FromInclusive {
				op = "gt"
			}
			conds = append(conds, KVPair{op, *e.From})
		}
		if e.To != nil {
			op := "lte"
			if !e.ToInclusive {
				op = "lt"
			}
			conds = append(conds, KVPair{op, *e.To})
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

// Neq is an AST node for inequality.
type Neq[T any] struct {
	Ident string
	Value T
}

// Idents returns all the identifiers in the expression.
func (e Neq[T]) Idents() []string {
	return []string{e.Ident}
}

// Map returns the query map corresponding to the expression.
func (e Neq[T]) Map(fl ExprFlavour) (interface{}, error) {
	switch fl {
	case DocDB:
		return nil, errors.ErrUnsupported
	case OpenSearch:
		return Map{Pairs: []KVPair{
			{"bool", Map{Pairs: []KVPair{
				{"must_not", Map{Pairs: []KVPair{
					{"term", Map{Pairs: []KVPair{
						{e.Ident, e.Value},
					}}},
				}}},
			}}},
		}}, nil
	}
	panic("unknown expression flavour: " + fl.String())
}
