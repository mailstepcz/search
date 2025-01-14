package search

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_buildBulkBody(t *testing.T) {
	type testCase[T any] struct {
		name string
		ops  []BulkOperation[T]
		want []byte
	}

	type doc struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}

	tests := []testCase[doc]{{
		name: "2 index ops",
		ops: []BulkOperation[doc]{
			{
				OperationType: OpIndex,
				ID:            "ID-1",
				Index:         "test-index",
				Doc: &doc{
					Id:   "1",
					Name: "Doc 1",
				},
			}, {
				OperationType: OpIndex,
				ID:            "ID-2",
				Index:         "test-index",
				Doc: &doc{
					Id:   "2",
					Name: "Doc 2",
				},
			},
		},
		want: []byte(`{"index":{"_id":"ID-1","_index":"test-index"}}
{"id":"1","name":"Doc 1"}
{"index":{"_id":"ID-2","_index":"test-index"}}
{"id":"2","name":"Doc 2"}
`),
	}, {
		name: "index and delete",
		ops: []BulkOperation[doc]{
			{
				OperationType: OpIndex,
				ID:            "ID-1",
				Index:         "test-index",
				Doc: &doc{
					Id:   "1",
					Name: "Doc 1",
				},
			}, {
				OperationType: OpDelete,
				ID:            "ID-2",
				Index:         "test-index",
			},
		},
		want: []byte(`{"index":{"_id":"ID-1","_index":"test-index"}}
{"id":"1","name":"Doc 1"}
{"delete":{"_id":"ID-2","_index":"test-index"}}
`),
	}, {
		name: "index",
		ops: []BulkOperation[doc]{
			{
				OperationType: OpIndex,
				ID:            "ID-1",
				Index:         "test-index",
				Doc: &doc{
					Id:   "1",
					Name: "Doc 1",
				},
			},
		},
		want: []byte(`{"index":{"_id":"ID-1","_index":"test-index"}}
{"id":"1","name":"Doc 1"}
`),
	}, {
		name: "delete",
		ops: []BulkOperation[doc]{
			{
				OperationType: OpDelete,
				ID:            "ID-2",
				Index:         "test-index",
			},
		},
		want: []byte(`{"delete":{"_id":"ID-2","_index":"test-index"}}
`),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			got, err := buildBulkBody(tt.ops)

			req.NoError(err)
			req.Equal(tt.want, got)
		})
	}
}
