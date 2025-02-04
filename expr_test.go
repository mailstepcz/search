package search

import (
	"encoding/json"
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

func TestOpensearchWildcard(t *testing.T) {
	req := require.New(t)

	e := Wildcard{"field", "Ad Ba"}
	m, err := e.Map(OpenSearch)
	req.NoError(err)

	b, err := json.Marshal(m)
	req.NoError(err)

	req.Equal(`[{"wildcard":{"field":{"value":"*Ad*","case_insensitive":true}}},{"wildcard":{"field":{"value":"*Ba*","case_insensitive":true}}}]`, string(b))
}
