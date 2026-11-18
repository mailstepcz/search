package search

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// Bodies captured from OpenSearch 2.19.6.
const (
	// getMissingDocumentBody answers a get of a missing document with HTTP 404.
	getMissingDocumentBody = `{"_index":"zz-probe-not-found-308221","_id":"missing","found":false}`
	// deleteMissingDocumentBody answers a delete of a missing document with HTTP 404.
	deleteMissingDocumentBody = `{"_index":"zz-probe-not-found-308221","_id":"missing","_version":1,"result":"not_found","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}`
	// updateMissingDocumentBody answers an update of a missing document with HTTP 404.
	updateMissingDocumentBody = `{"error":{"root_cause":[{"type":"document_missing_exception","reason":"[missing]: document missing","index":"zz-probe-not-found-308221","shard":"0","index_uuid":"zhEER7MWTH2C06NIvkkOiQ"}],"type":"document_missing_exception","reason":"[missing]: document missing","index":"zz-probe-not-found-308221","shard":"0","index_uuid":"zhEER7MWTH2C06NIvkkOiQ"},"status":404}`
	// missingIndexBody answers a request on a missing index with HTTP 404.
	missingIndexBody = `{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index [zz-no-such-index-308221]","index":"zz-no-such-index-308221","resource.id":"zz-no-such-index-308221","resource.type":"index_expression","index_uuid":"_na_"}],"type":"index_not_found_exception","reason":"no such index [zz-no-such-index-308221]","index":"zz-no-such-index-308221","resource.id":"zz-no-such-index-308221","resource.type":"index_expression","index_uuid":"_na_"},"status":404}`
	// missingIndexAutoCreateOffBody answers an index or update on a missing index that auto-create must not create with HTTP 404.
	missingIndexAutoCreateOffBody = `{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index [zz-noauto-468811-x] and [action.auto_create_index] contains [-zz-noauto-468811-*] which forbids automatic creation of the index","index":"zz-noauto-468811-x","index_uuid":"_na_"}],"type":"index_not_found_exception","reason":"no such index [zz-noauto-468811-x] and [action.auto_create_index] contains [-zz-noauto-468811-*] which forbids automatic creation of the index","index":"zz-noauto-468811-x","index_uuid":"_na_"},"status":404}`
	// versionConflictBody answers an index or update of an older external version with HTTP 409.
	versionConflictBody = `{"error":{"root_cause":[{"type":"version_conflict_engine_exception","reason":"[v]: version conflict, current version [500] is higher than the one provided [300]","index":"zz-noauto-468811-new","shard":"0","index_uuid":"LJbuR0ohT9CXbPS_6PUAAw"}],"type":"version_conflict_engine_exception","reason":"[v]: version conflict, current version [500] is higher than the one provided [300]","index":"zz-noauto-468811-new","shard":"0","index_uuid":"LJbuR0ohT9CXbPS_6PUAAw"},"status":409}`
)

// Made-up bodies.
const (
	// otherNotFoundBody is a 404 that names neither a missing document nor a missing index.
	otherNotFoundBody = `{"error":{"root_cause":[{"type":"resource_not_found_exception","reason":"no such resource"}],"type":"resource_not_found_exception","reason":"no such resource"},"status":404}`
	// otherNotFoundWithDocumentFieldsBody is another 404 whose error comes with the fields of a missing document.
	otherNotFoundWithDocumentFieldsBody = `{"error":{"root_cause":[{"type":"resource_not_found_exception","reason":"no such resource"}],"type":"resource_not_found_exception","reason":"no such resource"},"status":404,"found":false,"result":"not_found"}`
	// proxyNotFoundBody is the 404 page of a proxy in front of OpenSearch.
	proxyNotFoundBody = `<html><head><title>404 Not Found</title></head><body><h1>404 Not Found</h1></body></html>`
	// forbiddenBody is a 403 of a role without the permission for the request.
	forbiddenBody = `{"error":{"root_cause":[{"type":"security_exception","reason":"no permissions"}],"type":"security_exception","reason":"no permissions"},"status":403}`
)

// errorResponseCase is one error response of OpenSearch; want is the only specific sentinel the returned error carries, nil for none.
type errorResponseCase struct {
	name       string
	statusCode int
	body       string
	want       error
}

func TestGet(t *testing.T) {
	for _, tc := range []errorResponseCase{
		{name: "a missing document - should fail", statusCode: http.StatusNotFound, body: getMissingDocumentBody, want: ErrDocumentNotFound},
		{name: "a missing index - should fail", statusCode: http.StatusNotFound, body: missingIndexBody, want: ErrIndexNotFound},
		{name: "another 404 - should fail", statusCode: http.StatusNotFound, body: otherNotFoundBody, want: nil},
		{name: "a server error with the body of a missing document - should fail", statusCode: http.StatusInternalServerError, body: getMissingDocumentBody, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			req := require.New(t)

			_, err := Get[map[string]any](ctx, newFakeClient(t, tc.statusCode, tc.body), "test-index", "1")
			req.ErrorIs(err, ErrOpensearchRequestFailed)
			requireOnlySentinel(t, err, tc.want)
		})
	}
}

func TestDelete(t *testing.T) {
	for _, tc := range []errorResponseCase{
		{name: "a missing document - should fail", statusCode: http.StatusNotFound, body: deleteMissingDocumentBody, want: ErrDocumentNotFound},
		{name: "a missing index - should fail", statusCode: http.StatusNotFound, body: missingIndexBody, want: ErrIndexNotFound},
		{name: "another 404 - should fail", statusCode: http.StatusNotFound, body: otherNotFoundBody, want: nil},
		{name: "a 404 page of a proxy - should fail", statusCode: http.StatusNotFound, body: proxyNotFoundBody, want: nil},
		{name: "a server error with the body of a missing document - should fail", statusCode: http.StatusInternalServerError, body: deleteMissingDocumentBody, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			req := require.New(t)

			err := Delete(ctx, newFakeClient(t, tc.statusCode, tc.body), "test-index", "1")
			req.ErrorIs(err, ErrOpensearchRequestFailed)
			requireOnlySentinel(t, err, tc.want)
		})
	}
}

func TestUpdate(t *testing.T) {
	for _, tc := range []errorResponseCase{
		{name: "a missing document - should fail", statusCode: http.StatusNotFound, body: updateMissingDocumentBody, want: ErrDocumentNotFound},
		{name: "a missing index - should fail", statusCode: http.StatusNotFound, body: missingIndexAutoCreateOffBody, want: ErrIndexNotFound},
		{name: "another 404 - should fail", statusCode: http.StatusNotFound, body: otherNotFoundBody, want: nil},
		{name: "another 404 with the fields of a missing document - should fail", statusCode: http.StatusNotFound, body: otherNotFoundWithDocumentFieldsBody, want: nil},
		{name: "an older version - should fail", statusCode: http.StatusConflict, body: versionConflictBody, want: ErrDocumentHasNewerVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			req := require.New(t)

			err := Update(ctx, newFakeClient(t, tc.statusCode, tc.body), "test-index", "1", &map[string]any{"name": "Doc 1"})
			req.ErrorIs(err, ErrOpensearchRequestFailed)
			requireOnlySentinel(t, err, tc.want)
		})
	}
}

func TestIndex(t *testing.T) {
	for _, tc := range []errorResponseCase{
		{name: "a missing index - should fail", statusCode: http.StatusNotFound, body: missingIndexAutoCreateOffBody, want: ErrIndexNotFound},
		{name: "an older version - should fail", statusCode: http.StatusConflict, body: versionConflictBody, want: ErrDocumentHasNewerVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			req := require.New(t)

			err := Index(ctx, newFakeClient(t, tc.statusCode, tc.body), "test-index", "1", &map[string]any{"name": "Doc 1"})
			req.ErrorIs(err, ErrOpensearchRequestFailed)
			requireOnlySentinel(t, err, tc.want)
		})
	}
}

func TestSearch(t *testing.T) {
	for _, tc := range []errorResponseCase{
		{name: "a missing index - should fail", statusCode: http.StatusNotFound, body: missingIndexBody, want: ErrIndexNotFound},
		{name: "a server error with the body of a missing index - should fail", statusCode: http.StatusInternalServerError, body: missingIndexBody, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			req := require.New(t)

			_, _, err := Search[map[string]any](ctx, newFakeClient(t, tc.statusCode, tc.body), "test-index", Eq[string]{Ident: "name", Value: "Doc 1"}, "", nil)
			req.ErrorIs(err, ErrOpensearchRequestFailed)
			requireOnlySentinel(t, err, tc.want)
		})
	}
}

func TestBuildQueryResultWindow(t *testing.T) {
	expr := Eq[int]{Ident: "a", Value: 1234}

	tests := []struct {
		name    string
		pag     *Pagination
		wantErr bool
	}{
		{name: "nil pagination", pag: nil, wantErr: false},
		{name: "shallow window", pag: &Pagination{From: 0, Size: 10}, wantErr: false},
		{name: "at limit", pag: &Pagination{From: 9990, Size: 10}, wantErr: false},
		{name: "over limit", pag: &Pagination{From: 9990, Size: 20}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)

			_, err := buildQuery(expr, "", tt.pag)
			if tt.wantErr {
				req.Error(err)
				req.ErrorIs(err, ErrResultWindowExceeded)
			} else {
				req.NoError(err)
			}
		})
	}
}

// requireOnlySentinel checks that err carries want and no other of the specific sentinels; a nil want means none of them.
func requireOnlySentinel(t *testing.T, err, want error) {
	t.Helper()

	for _, sentinel := range []error{ErrDocumentNotFound, ErrIndexNotFound, ErrDocumentHasNewerVersion, ErrBulkItemError} {
		require.Equal(t, sentinel == want, errors.Is(err, sentinel), sentinel.Error())
	}
}
