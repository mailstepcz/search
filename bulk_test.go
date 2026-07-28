package search

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/stretchr/testify/require"
)

type versionedDoc struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (d *versionedDoc) Version() int             { return 1234 }
func (d *versionedDoc) VersionType() VersionType { return VersionTypeExternalGTE }

func Test_buildBulkBody(t *testing.T) {
	type testCase[T any] struct {
		name string
		ops  []BulkOperation[T]
		want []byte
	}

	type doc struct {
		ID   string `json:"id"`
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
					ID:   "1",
					Name: "Doc 1",
				},
			}, {
				OperationType: OpIndex,
				ID:            "ID-2",
				Index:         "test-index",
				Doc: &doc{
					ID:   "2",
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
					ID:   "1",
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
					ID:   "1",
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
			var b bytes.Buffer
			err := buildBulkBody(tt.ops, &b)

			req.NoError(err)
			req.Equal(tt.want, b.Bytes())
		})
	}
}

func Test_buildBulkBody_versionedDocument(t *testing.T) {
	req := require.New(t)

	ops := []BulkOperation[versionedDoc]{
		{
			OperationType: OpIndex,
			ID:            "ID-1",
			Index:         "test-index",
			Doc:           &versionedDoc{ID: "1", Name: "Versioned Doc"},
		},
		{
			OperationType: OpDelete,
			ID:            "ID-2",
			Index:         "test-index",
		},
	}

	// index op: action metadata must include version and version_type (keys sorted alphabetically).
	// delete op: Doc is nil — no version fields.
	want := []byte(
		`{"index":{"_id":"ID-1","_index":"test-index","version":1234,"version_type":"external_gte"}}` + "\n" +
			`{"id":"1","name":"Versioned Doc"}` + "\n" +
			`{"delete":{"_id":"ID-2","_index":"test-index"}}` + "\n",
	)

	var b bytes.Buffer
	err := buildBulkBody(ops, &b)
	req.NoError(err)
	req.Equal(want, b.Bytes())
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newFakeClient(t *testing.T, statusCode int, body string) *opensearch.Client {
	t.Helper()

	cl, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"http://localhost:9200"},
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: statusCode,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}),
	})
	require.NoError(t, err)
	return cl
}

func TestBulk(t *testing.T) {
	t.Run("partial item failures are classified per status", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		body := `{
			"took": 3,
			"errors": true,
			"items": [
				{"index": {"_index": "test-index", "_id": "1", "status": 201}},
				{"index": {"_index": "test-index", "_id": "2", "status": 409, "error": {"type": "version_conflict_engine_exception", "reason": "version conflict"}}},
				{"delete": {"_index": "test-index", "_id": "3", "status": 404, "error": {"type": "not_found", "reason": "document missing"}}},
				{"index": {"_index": "test-index", "_id": "4", "status": 400, "error": {"type": "mapper_parsing_exception", "reason": "bad mapping"}}}
			]
		}`
		cl := newFakeClient(t, http.StatusOK, body)

		result, err := Bulk(ctx, cl, []BulkOperation[any]{
			{OperationType: OpIndex, ID: "1", Index: "test-index"},
		})
		req.NoError(err)
		req.Len(result.Items, 3)

		byID := make(map[string]BulkItemResult, len(result.Items))
		for _, item := range result.Items {
			byID[item.ID] = item
		}

		req.ErrorIs(byID["2"].Error, ErrDocumentHasNewerVersion)
		req.ErrorIs(byID["3"].Error, ErrDocumentNotFound)
		req.ErrorIs(byID["4"].Error, ErrBulkItemError)
	})

	t.Run("no item failures returns an empty result", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		body := `{"took": 1, "errors": false, "items": [{"index": {"_index": "test-index", "_id": "1", "status": 201}}]}`
		cl := newFakeClient(t, http.StatusOK, body)

		result, err := BulkWithRefresh(ctx, cl, []BulkOperation[any]{
			{OperationType: OpIndex, ID: "1", Index: "test-index"},
		})
		req.NoError(err)
		req.Empty(result.Items)
	})

	t.Run("whole request fails at HTTP level - should fail", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		cl := newFakeClient(t, http.StatusInternalServerError, `{"error": "internal server error"}`)

		result, err := Bulk(ctx, cl, []BulkOperation[any]{
			{OperationType: OpIndex, ID: "1", Index: "test-index"},
		})
		req.Nil(result)
		req.ErrorIs(err, ErrOpensearchRequestFailed)
	})
}
