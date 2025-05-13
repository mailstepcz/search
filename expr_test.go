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

func TestOpensearchIntervalGtLt(t *testing.T) {
	req := require.New(t)

	from, to := 10, 15
	e := Interval[int]{Ident: "a", From: &from, FromInclusive: false, To: &to, ToInclusive: false}
	m, err := e.Map(OpenSearch)
	req.NoError(err)
	req.Equal(Map{[]KVPair{{"range", Map{[]KVPair{{"a", Map{Pairs: []KVPair{{Key: "gt", Value: from}, {Key: "lt", Value: to}}}}}}}}}, m)
}

func TestOpensearchIntervalGteLte(t *testing.T) {
	req := require.New(t)

	from, to := 10, 15
	e := Interval[int]{Ident: "a", From: &from, FromInclusive: true, To: &to, ToInclusive: true}
	m, err := e.Map(OpenSearch)
	req.NoError(err)
	req.Equal(Map{[]KVPair{{"range", Map{[]KVPair{{"a", Map{Pairs: []KVPair{{Key: "gte", Value: from}, {Key: "lte", Value: to}}}}}}}}}, m)
}

func TestOpensearchTerms(t *testing.T) {
	req := require.New(t)

	e := Terms[string]{Ident: "activity", Values: []string{"activity1", "activity2"}}
	m, err := e.Map(OpenSearch)
	req.NoError(err)
	req.Equal(Map{[]KVPair{{"terms", Map{[]KVPair{{"activity", []string{"activity1", "activity2"}}}}}}}, m)
}

func TestOpensearchWildcard(t *testing.T) {
	req := require.New(t)
	e := Wildcard{Ident: "username", Value: "john"}
	m, err := e.Map(OpenSearch)
	req.NoError(err)
	req.Equal(Map{[]KVPair{
		{"wildcard", Map{[]KVPair{
			{"username", Map{[]KVPair{
				{"value", "*john*"},
				{"case_insensitive", true},
			}}},
		}}},
	}}, m)
}
