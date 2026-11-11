package search

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/stretchr/testify/require"
)

func TestAliasGet(t *testing.T) {
	t.Run("the alias is looked up with the alias named as the index", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		var path string
		cl := newFakeAPIClient(t, func(r *http.Request) (int, string) {
			path = r.URL.Path
			return http.StatusOK, `{"products-1": {"aliases": {"products": {}}}}`
		})

		alias, err := AliasGet(ctx, cl, "products")
		req.NoError(err)
		req.Equal("/products/_alias/products", path)
		req.Equal(&Alias{Name: "products", Index: "products-1"}, alias)
	})

	t.Run("a missing alias - should fail", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		cl := newFakeAPIClient(t, func(*http.Request) (int, string) {
			return http.StatusNotFound, `{"error": "alias [products] missing", "status": 404}`
		})

		_, err := AliasGet(ctx, cl, "products")
		req.ErrorIs(err, ErrAliasNotFound)
	})

	t.Run("an alias on two indices - should fail", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		cl := newFakeAPIClient(t, func(*http.Request) (int, string) {
			return http.StatusOK, `{"products-1": {"aliases": {"products": {}}}, "products-2": {"aliases": {"products": {}}}}`
		})

		_, err := AliasGet(ctx, cl, "products")
		req.ErrorContains(err, "unexpected index count for alias")
		req.NotErrorIs(err, ErrAliasNotFound)
	})

	t.Run("a forbidden lookup is not reported as a missing alias - should fail", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		cl := newFakeAPIClient(t, func(*http.Request) (int, string) {
			return http.StatusForbidden, `{"error": {"root_cause": [{"type": "security_exception", "reason": "no permissions"}], "type": "security_exception", "reason": "no permissions"}, "status": 403}`
		})

		_, err := AliasGet(ctx, cl, "products")
		req.Error(err)
		req.NotErrorIs(err, ErrAliasNotFound)
	})
}

func TestAliasSwitch(t *testing.T) {
	t.Run("the alias moves from the old index to the new one in one request", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		var aliasesBody string
		cl := newFakeAPIClient(t, func(r *http.Request) (int, string) {
			if r.Method == http.MethodGet {
				return http.StatusOK, `{"products-1": {"aliases": {"products": {}}}}`
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				return http.StatusInternalServerError, err.Error()
			}
			aliasesBody = string(b)
			return http.StatusOK, `{"acknowledged": true}`
		})

		req.NoError(AliasSwitch(ctx, cl, "products", "products-2"))
		req.JSONEq(`{"actions": [{"add": {"index": "products-2", "alias": "products"}}, {"remove": {"index": "products-1", "alias": "products"}}]}`, aliasesBody)
	})
}

func newFakeAPIClient(t *testing.T, respond func(r *http.Request) (int, string)) *opensearchapi.Client {
	t.Helper()

	cl, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{"http://localhost:9200"},
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				statusCode, body := respond(r)
				return &http.Response{
					StatusCode: statusCode,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(body)),
				}, nil
			}),
		},
	})
	require.NoError(t, err)
	return cl
}
