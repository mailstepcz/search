// Package search is a thin API layer for fulltext searching with Opensearch.
package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/mailstepcz/serr"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	requestsigner "github.com/opensearch-project/opensearch-go/v4/signer/awsv2"
)

var (
	// ErrOpensearchRequestFailed represents an error from the Opensearch client.
	ErrOpensearchRequestFailed = errors.New("OpenSearch error")
	// ErrOpensearchBadRequest signifies that a JSON request for Opensearch is ill-formed.
	ErrOpensearchBadRequest = errors.New("OpenSearch bad request")
)

// NewClient creates a new OpenSearch client.
func NewClient(ctx context.Context, cfg opensearch.Config) (*opensearch.Client, error) {
	cl, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

// NewAWSClient creates a new OpenSearch client for an AWS hosted engine.
func NewAWSClient(ctx context.Context, url string, awsCfg aws.Config) (*opensearch.Client, error) {
	signer, err := requestsigner.NewSignerWithService(awsCfg, "es")
	if err != nil {
		return nil, err
	}
	return opensearch.NewClient(opensearch.Config{
		Addresses: []string{url},
		Signer:    signer,
	})
}

// Index indexes a document.
func Index[T any](ctx context.Context, cl *opensearch.Client, index, id string, doc *T) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	req := opensearchapi.IndexReq{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(b),
	}
	resp, err := cl.Do(ctx, req, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return osError(resp)
	}
	return nil
}

// Update indexes a document.
func Update[T any](ctx context.Context, cl *opensearch.Client, index, id string, doc *T) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	req := opensearchapi.UpdateReq{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(b),
	}
	resp, err := cl.Do(ctx, req, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return osError(resp)
	}
	return nil
}

// Delete deletes a document.
func Delete(ctx context.Context, cl *opensearch.Client, index, id string) error {
	req := opensearchapi.DocumentDeleteReq{
		Index:      index,
		DocumentID: id,
	}
	resp, err := cl.Do(ctx, req, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return osError(resp)
	}
	return nil
}

// Search searches for documents.
// The orderBy argument is the column by which to order the results. A hyphen at its beginning signifies descending order.
func Search[T any](ctx context.Context, cl *opensearch.Client, index string, expr Expr, orderBy string, pag *Pagination) ([]IDedDocument[T], int, error) {
	maps, err := expr.Map(OpenSearch)
	if err != nil {
		return nil, 0, err
	}
	if m, ok := maps.(Map); ok {
		maps = []Map{m} // the "must" attribute shall be an array
	}
	arr, ok := maps.([]Map)
	if !ok {
		return nil, 0, fmt.Errorf("%w %T", ErrOpensearchBadRequest, maps)
	}
	q := searchQuery{
		Query: searchBool{
			Bool: searchMust{
				Must: arr,
			},
		},
	}
	if orderBy != "" {
		dir := "asc"
		if orderBy[0] == '-' {
			dir = "desc"
			orderBy = orderBy[1:]
		}
		field := orderBy
		q.Sort = []map[string]interface{}{
			{
				field: map[string]string{
					"order": dir,
				},
			},
		}
	}
	if pag != nil {
		q.From = &pag.From
		q.Size = &pag.Size
	}
	b, err := json.Marshal(q)
	if err != nil {
		return nil, 0, err
	}
	content := bytes.NewReader(b)
	req := opensearchapi.SearchReq{
		Indices: []string{index},
		Body:    content,
	}
	var sresp opensearchapi.SearchResp
	resp, err := cl.Do(ctx, req, &sresp)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode >= 400 {
		return nil, 0, osError(resp)
	}
	total := sresp.Hits.Total.Value
	docs := make([]IDedDocument[T], 0, len(sresp.Hits.Hits))
	for _, h := range sresp.Hits.Hits {
		var doc T
		if err := json.Unmarshal(h.Source, &doc); err != nil {
			return nil, 0, err
		}
		docs = append(docs, IDedDocument[T]{
			ID:       h.ID,
			Document: &doc,
		})
	}
	return docs, total, nil
}

func osError(resp *opensearch.Response) error {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return serr.Wrap("search failed", ErrOpensearchRequestFailed, serr.Int("statusCode", resp.StatusCode), serr.String("body", string(b)))
}

func getCredentialProvider(accessKey, secretAccessKey string) aws.CredentialsProviderFunc {
	return func(ctx context.Context) (aws.Credentials, error) {
		c := &aws.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretAccessKey,
		}
		return *c, nil
	}
}

// Pagination contains the offset and limit for searches.
type Pagination struct {
	From int
	Size int
}

// IDedDocument is an IDed document.
type IDedDocument[T any] struct {
	ID       string
	Document *T
}

type searchQuery struct {
	Query searchBool               `json:"query"`
	Sort  []map[string]interface{} `json:"sort,omitempty"`
	From  *int                     `json:"from,omitempty"`
	Size  *int                     `json:"size,omitempty"`
}

type searchBool struct {
	Bool searchMust `json:"bool"`
}

type searchMust struct {
	Must interface{} `json:"must"`
}
