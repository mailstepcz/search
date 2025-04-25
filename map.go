package search

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// KVPair is a key-value pair.
type KVPair struct {
	Key   string
	Value interface{}
}

func (p KVPair) String() string {
	return fmt.Sprintf("%s: %v", p.Key, p.Value)
}

func (p KVPair) appendJSON(b []byte) []byte {
	b = strconv.AppendQuote(b, p.Key)
	b = append(b, ':')
	return appendValue(b, p.Value)
}

func appendValue(b []byte, x interface{}) []byte {
	switch x := x.(type) {
	case bool:
		return strconv.AppendBool(b, x)
	case int:
		return strconv.AppendInt(b, int64(x), 10)
	case float32:
		return strconv.AppendFloat(b, float64(x), 'e', -1, 32)
	case float64:
		return strconv.AppendFloat(b, x, 'e', -1, 64)
	case string:
		return strconv.AppendQuote(b, x)
	case uuid.UUID:
		return strconv.AppendQuote(b, x.String())
	case time.Time:
		return strconv.AppendQuote(b, x.Format(time.RFC3339))
	case []string:
		b = append(b, '[')
		for i, x := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendValue(b, x)
		}
		return append(b, ']')
	case []any:
		b = append(b, '[')
		for i, x := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendValue(b, x)
		}
		return append(b, ']')
	case Map:
		return x.appendJSON(b)
	}
	panic(fmt.Sprintf("unknown value type: %v (%T)", x, x))
}

// Map is an associated array.
// The aim of this implementation is effectivity, it shall be used only within the search package.
// Duplicate keys aren't checked.
type Map struct {
	Pairs []KVPair
}

func (m Map) appendJSON(b []byte) []byte {
	b = append(b, '{')
	for i, p := range m.Pairs {
		if i > 0 {
			b = append(b, ',')
		}
		b = p.appendJSON(b)
	}
	return append(b, '}')
}

// MarshalJSON marshals the map into JSON.
func (m Map) MarshalJSON() ([]byte, error) {
	return m.JSON(), nil
}

var _ json.Marshaler = Map{}

// JSON returns the JSON representation of the map.
func (m Map) JSON() []byte {
	b := make([]byte, 0, 100)
	return m.appendJSON(b)
}
