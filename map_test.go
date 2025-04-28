package search

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMapJSON(t *testing.T) {
	req := require.New(t)

	ts := time.Time{}.Add((24*365 + 1) * time.Hour)
	m := Map{Pairs: []KVPair{
		{"a", 1234},
		{"b", 12.34},
		{"c", true},
		{"d", "abcdefgh"},
		{"e", ts},
		{"f", []interface{}{"abcd", 1234}},
		{"g", Map{Pairs: []KVPair{
			{"a", 1},
			{"b", "2"},
		}}},
		{"h", []string{"hello world"}},
		{"k", []uuid.UUID{uuid.MustParse("52eab613-58a6-498c-8947-781eeba0011d")}},
	}}

	b := m.JSON()

	req.Equal(`{"a":1234,"b":1.234e+01,"c":true,"d":"abcdefgh","e":"0002-01-01T01:00:00Z","f":["abcd",1234],"g":{"a":1,"b":"2"},"h":["hello world"],"k":["52eab613-58a6-498c-8947-781eeba0011d"]}`, string(b))

	var m2 map[string]interface{}
	err := json.Unmarshal(b, &m2)
	req.NoError(err)
}

var gr interface{}

func BenchmarkStdlibMarshalling(b *testing.B) {
	ts := time.Time{}.Add((24*365 + 1) * time.Hour)
	var lr interface{}
	for i := 0; i < b.N; i++ {
		m := map[string]interface{}{
			"a": 1234,
			"b": 12.34,
			"c": true,
			"d": "abcdefgh",
			"e": ts,
			"f": []interface{}{"abcd", 1234},
			"g": map[string]interface{}{
				"a": 1,
				"b": "2",
			},
		}
		bs, err := json.Marshal(m)
		if err != nil {
			b.Fatal(err)
		}
		lr = bs
	}
	gr = lr
}

func BenchmarkOurMarshalling(b *testing.B) {
	ts := time.Time{}.Add((24*365 + 1) * time.Hour)
	var lr interface{}
	for i := 0; i < b.N; i++ {
		m := Map{Pairs: []KVPair{
			{"a", 1234},
			{"b", 12.34},
			{"c", true},
			{"d", "abcdefgh"},
			{"e", ts},
			{"f", []interface{}{"abcd", 1234}},
			{"g", Map{Pairs: []KVPair{
				{"a", 1},
				{"b", "2"},
			}}},
		}}
		bs := m.JSON()
		lr = bs
	}
	gr = lr
}
