package search

import (
	"encoding/json"
	"testing"
	"time"

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
	}}

	b := m.JSON()

	req.Equal(`{"a":1234,"b":1.234e+01,"c":true,"d":"abcdefgh","e":"0002-01-01T01:00:00Z","f":["abcd",1234],"g":{"a":1,"b":"2"}}`, string(b))

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
