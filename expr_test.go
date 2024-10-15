package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDocDBEq(t *testing.T) {
	req := require.New(t)

	e := Eq[int]{Ident: "a", Value: 1234}
	m, err := e.Map(DocDB)
	req.NoError(err)
	req.Equal(Map{[]KVPair{{"a", 1234}}}, m)
}

func TestOpensearchEq(t *testing.T) {
	req := require.New(t)

	e := Eq[int]{Ident: "a", Value: 1234}
	m, err := e.Map(OpenSearch)
	req.NoError(err)
	req.Equal(Map{[]KVPair{{"term", Map{[]KVPair{{"a", 1234}}}}}}, m)
}

func TestDocDBAnd(t *testing.T) {
	req := require.New(t)

	e := And{Exprs: []Expr{Eq[int]{Ident: "a", Value: 1234}, Eq[int]{Ident: "b", Value: 5678}}}
	m, err := e.Map(DocDB)
	req.NoError(err)
	req.Equal(Map{[]KVPair{{"a", 1234}, {"b", 5678}}}, m)
}

func TestOpensearchAnd(t *testing.T) {
	req := require.New(t)

	e := And{Exprs: []Expr{Eq[int]{Ident: "a", Value: 1234}, Match{Ident: "b", Value: "5678"}}}
	m, err := e.Map(OpenSearch)
	req.NoError(err)
	req.Equal([]Map{
		{[]KVPair{{"term", Map{[]KVPair{{"a", 1234}}}}}},
		{[]KVPair{{"match", Map{[]KVPair{{"b", "5678"}}}}}},
	}, m)
}
