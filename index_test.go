package search

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexExists(t *testing.T) {
	t.Run("an existing index", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		var request string
		cl := newFakeAPIClient(t, func(r *http.Request) (int, string) {
			request = r.Method + " " + r.URL.Path
			return http.StatusOK, ``
		})

		exists, err := IndexExists(ctx, cl, "products-1")
		req.NoError(err)
		req.True(exists)
		req.Equal("HEAD /products-1", request)
	})

	t.Run("a missing index", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		cl := newFakeAPIClient(t, func(*http.Request) (int, string) {
			return http.StatusNotFound, ``
		})

		exists, err := IndexExists(ctx, cl, "products-1")
		req.NoError(err)
		req.False(exists)
	})

	t.Run("a forbidden check is not reported as a missing index - should fail", func(t *testing.T) {
		ctx := t.Context()
		req := require.New(t)

		cl := newFakeAPIClient(t, func(*http.Request) (int, string) {
			return http.StatusForbidden, ``
		})

		_, err := IndexExists(ctx, cl, "products-1")
		req.ErrorContains(err, "checking index existence")
	})
}
