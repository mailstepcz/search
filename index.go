package search

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/mailstepcz/serr"
)

type indexCreateBody struct {
	Settings json.RawMessage `json:"settings"`
	Mappings json.RawMessage `json:"mappings,omitempty"`
}

// IndexCreate creates index with given name.
func IndexCreate(ctx context.Context, client *opensearchapi.Client, index string, mapping, setting json.RawMessage) error {
	b, err := json.Marshal(indexCreateBody{
		Mappings: mapping,
		Settings: setting,
	})
	if err != nil {
		return serr.Wrap("marshalling index create body", err)
	}

	req := opensearchapi.IndicesCreateReq{
		Index: index,
		Body:  bytes.NewReader(b),
	}

	resp, err := client.Indices.Create(ctx, req)
	if err != nil {
		return serr.Wrap("creating index", err, serr.String("index", index))
	}

	if !resp.Acknowledged {
		return serr.New("new index not acknowledged", serr.String("index", index))
	}

	return nil
}

// IndexDelete deletes index and it's content.
func IndexDelete(ctx context.Context, client *opensearchapi.Client, index string) error {
	resp, err := client.Indices.Delete(ctx, opensearchapi.IndicesDeleteReq{
		Indices: []string{index},
	})
	if err != nil {
		return serr.Wrap("deleting index", err, serr.String("index", index))
	}
	if !resp.Acknowledged {
		return serr.New("index delete not acknowledged", serr.String("index", index))
	}

	return nil
}

// IndexExists reports whether the index exists.
func IndexExists(ctx context.Context, client *opensearchapi.Client, index string) (bool, error) {
	resp, err := client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{index}})
	if resp != nil && resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if err != nil {
		return false, serr.Wrap("checking index existence", err, serr.String("index", index))
	}

	return true, nil
}
