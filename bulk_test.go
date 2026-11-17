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
	t.Run("failed items are classified by their status and error type", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		// item 1 succeeded and item 5, a delete of a missing document, carries no error: neither is a failed item
		body := `{
			"took": 3,
			"errors": true,
			"items": [
				{"index": {"_index": "test-index", "_id": "1", "status": 201}},
				{"index": {"_index": "test-index", "_id": "2", "status": 409, "error": {"type": "version_conflict_engine_exception", "reason": "version conflict"}}},
				{"update": {"_index": "test-index", "_id": "3", "status": 404, "error": {"type": "document_missing_exception", "reason": "[3]: document missing", "index": "test-index", "shard": "0", "index_uuid": "zhEER7MWTH2C06NIvkkOiQ"}}},
				{"delete":{"_index":"zz-no-such-index-b-353730-d","_id":"4","status":404,"error":{"type":"index_not_found_exception","reason":"no such index [zz-no-such-index-b-353730-d]","index":"zz-no-such-index-b-353730-d","resource.id":"zz-no-such-index-b-353730-d","resource.type":"index_expression","index_uuid":"_na_"}}},
				{"delete": {"_index": "test-index", "_id": "5", "_version": 1, "result": "not_found", "_shards": {"total": 1, "successful": 1, "failed": 0}, "_seq_no": 0, "_primary_term": 1, "status": 404}},
				{"update": {"_index": "test-index", "_id": "6", "status": 404, "error": {"type": "resource_not_found_exception", "reason": "no such resource"}}},
				{"index": {"_index": "test-index", "_id": "7", "status": 400, "error": {"type": "mapper_parsing_exception", "reason": "bad mapping"}}},
				{"update": {"_index": "test-index", "_id": "8", "status": 400, "error": {"type": "document_missing_exception", "reason": "[8]: document missing"}}},
				{"delete": {"_index": "test-index", "_id": "9", "status": 400, "error": {"type": "index_not_found_exception", "reason": "no such index [test-index]"}}}
			]
		}`
		want := map[string]error{
			"2": ErrDocumentHasNewerVersion,
			"3": ErrDocumentNotFound, // update of a missing document
			"4": ErrIndexNotFound,    // delete on a missing index
			"6": ErrBulkItemError,    // another 404
			"7": ErrBulkItemError,
			"8": ErrBulkItemError, // a missing document type without the 404
			"9": ErrBulkItemError, // a missing index type without the 404
		}

		result, err := Bulk(ctx, newFakeClient(t, http.StatusOK, body), []BulkOperation[any]{
			{OperationType: OpIndex, ID: "1", Index: "test-index"},
		})
		req.NoError(err)
		req.Len(result.Items, len(want))
		for _, item := range result.Items {
			requireOnlySentinel(t, item.Error, want[item.ID])
		}
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
