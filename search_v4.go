// Package search is a thin API layer for fulltext searching with Opensearch.
package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/mailstepcz/pointer"
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
	// ErrDocumentNotFound signifies that no document was found.
	ErrDocumentNotFound = errors.New("document not found")

	// ErrScrollDeleteFailed represents an error from OpenSearch that scroll delete request was not successful.
	ErrScrollDeleteFailed = errors.New("OpenSearch scroll delete request failed")
	// ErrScrollNotFreed represents an error from OpenSearch when scroll delete request was
	// successful, but requested scroll was not freed.
	ErrScrollNotFreed = errors.New("OpenSearch scroll was not freed")
	// ErrScrollNotDefined OpenSearch scrollID is not defined.
	ErrScrollNotDefined = errors.New("OpenSearch scrollID is not defined")
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
	return indexDoc(ctx, cl, index, id, doc, nil)
}

// IndexWithRefresh indexes a document with refresh = true parameter.
// https://opensearch.org/docs/latest/api-reference/document-apis/index-document/#query-parameters
func IndexWithRefresh[T any](ctx context.Context, cl *opensearch.Client, index, id string, doc *T) error {
	return indexDoc(ctx, cl, index, id, doc, &opensearchapi.IndexParams{Refresh: "true"})
}

func indexDoc[T any](ctx context.Context, cl *opensearch.Client, index, id string, doc *T, params *opensearchapi.IndexParams) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	req := opensearchapi.IndexReq{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(b),
	}

	if params != nil {
		req.Params = *params
	}

	resp, err := cl.Do(ctx, req, nil)
	if err != nil {
		return err
	}
	if resp.IsError() {
		return osError(resp)
	}
	return nil
}

// Update indexes a document.
func Update[T any](ctx context.Context, cl *opensearch.Client, index, id string, doc *T, opts ...UpdateOption) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	var params *opensearchapi.UpdateParams
	// apply all options
	if len(opts) > 0 {
		params = new(opensearchapi.UpdateParams)
		for _, opt := range opts {
			opt(params)
		}
	}

	return updateDoc(ctx, cl, index, id, b, params)
}

// UpdateWithRefresh updates a document with refresh = true parameter.
// https://opensearch.org/docs/latest/api-reference/document-apis/update-document/#query-parameters
func UpdateWithRefresh[T any](ctx context.Context, cl *opensearch.Client, index, id string, doc *T) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return updateDoc(ctx, cl, index, id, b, &opensearchapi.UpdateParams{Refresh: "true"})
}

// UpdatePartial updates only specified fields on document.
func UpdatePartial(ctx context.Context, cl *opensearch.Client, index, id string, partialDoc map[string]any) error {
	payload := map[string]any{
		"doc": partialDoc,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return updateDoc(ctx, cl, index, id, b, nil)
}

// UpdatePartialWithRefresh updates only specified fields on document with refresh = true parameter.
// https://opensearch.org/docs/latest/api-reference/document-apis/update-document/#query-parameters
func UpdatePartialWithRefresh(ctx context.Context, cl *opensearch.Client, index, id string, partialDoc map[string]any) error {
	payload := map[string]any{
		"doc": partialDoc,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return updateDoc(ctx, cl, index, id, b, &opensearchapi.UpdateParams{Refresh: "true"})
}

func updateDoc(ctx context.Context, cl *opensearch.Client, index, id string, b []byte, params *opensearchapi.UpdateParams) error {
	req := opensearchapi.UpdateReq{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(b),
	}

	if params != nil {
		req.Params = *params
	}

	resp, err := cl.Do(ctx, req, nil)
	if err != nil {
		return err
	}
	if resp.IsError() {
		if resp.StatusCode == http.StatusNotFound {
			return errors.Join(ErrDocumentNotFound, osError(resp))
		}
		return osError(resp)
	}
	return nil
}

// Delete deletes a document.
func Delete(ctx context.Context, cl *opensearch.Client, index, id string) error {
	return deleteDoc(ctx, cl, index, id, nil)
}

// DeleteWithRefresh deletes a document with refresh = true parameter.
// https://opensearch.org/docs/latest/api-reference/document-apis/delete-document/#query-parameters
func DeleteWithRefresh(ctx context.Context, cl *opensearch.Client, index, id string) error {
	return deleteDoc(ctx, cl, index, id, &opensearchapi.DocumentDeleteParams{Refresh: "true"})
}

func deleteDoc(ctx context.Context, cl *opensearch.Client, index, id string, params *opensearchapi.DocumentDeleteParams) error {
	req := opensearchapi.DocumentDeleteReq{
		Index:      index,
		DocumentID: id,
	}

	if params != nil {
		req.Params = *params
	}

	resp, err := cl.Do(ctx, req, nil)
	if err != nil {
		return err
	}
	if resp.IsError() {
		if resp.StatusCode == http.StatusNotFound {
			return errors.Join(ErrDocumentNotFound, osError(resp))
		}
		return osError(resp)
	}
	return nil
}

// Get gets a document.
func Get[T any](ctx context.Context, cl *opensearch.Client, index, id string) (*T, error) {
	req := opensearchapi.DocumentGetReq{
		Index:      index,
		DocumentID: id,
	}
	var sresp opensearchapi.DocumentGetResp
	resp, err := cl.Do(ctx, req, &sresp)
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, osError(resp)
	}
	if !sresp.Found {
		return nil, ErrDocumentNotFound
	}
	var doc T
	if err := json.Unmarshal(sresp.Source, &doc); err != nil {
		return nil, err
	}
	return &doc, err
}

// Search searches for documents.
// The orderBy argument is the column by which to order the results. A hyphen at its beginning signifies descending order.
func Search[T any](ctx context.Context, cl *opensearch.Client, index string, expr Expr, orderBy string, pag *Pagination) ([]IDedDocument[T], int, error) {
	query, err := buildQuery(expr, orderBy, pag)
	if err != nil {
		return nil, 0, err
	}

	b, err := json.Marshal(query)
	if err != nil {
		return nil, 0, err
	}

	content := bytes.NewReader(b)
	req := opensearchapi.SearchReq{
		Indices: []string{index},
		Body:    content,
		Params:  opensearchapi.SearchParams{},
	}
	var osResp opensearchapi.SearchResp
	resp, err := cl.Do(ctx, req, &osResp)
	if err != nil {
		return nil, 0, err
	}
	if resp.IsError() {
		return nil, 0, osError(resp)
	}

	total := osResp.Hits.Total.Value

	docs, err := mapDocs[T](osResp.Hits.Hits)
	if err != nil {
		return nil, 0, err
	}

	return docs, total, nil
}

// Scroll starts new scroll on given index.
func Scroll[T any](ctx context.Context, cl *opensearch.Client, index string, expr Expr, orderBy string, size int, scrollWindow time.Duration) (*Scroller[T], error) {
	res, err := StartScroll[T](ctx, cl, index, expr, orderBy, size, scrollWindow)
	if err != nil {
		return nil, err
	}
	return newScroller(res, cl, scrollWindow), nil
}

// StartScroll starts new scroll that will returns results in batches of [size].
// Scroll is stable, and will be stable for given [ScrollWindow].
// When scroll is completed [StopScroll] to free up resources otherwise resources.
func StartScroll[T any](ctx context.Context, cl *opensearch.Client, index string, expr Expr, orderBy string, size int, scrollWindow time.Duration) (*ScrollResponse[T], error) {
	query, err := buildQuery(expr, orderBy, nil)
	if err != nil {
		return nil, err
	}

	query.Size = &size

	b, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	content := bytes.NewReader(b)
	req := opensearchapi.SearchReq{
		Indices: []string{index},
		Body:    content,
		Params: opensearchapi.SearchParams{
			Scroll: scrollWindow,
		},
	}
	var osResponse opensearchapi.SearchResp
	resp, err := cl.Do(ctx, req, &osResponse)
	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, osError(resp)
	}

	if osResponse.ScrollID == nil {
		return nil, ErrScrollNotDefined
	}

	total := osResponse.Hits.Total.Value

	docs, err := mapDocs[T](osResponse.Hits.Hits)
	if err != nil {
		return nil, err
	}

	return &ScrollResponse[T]{
		Docs:     docs,
		Total:    total,
		ScrollID: *osResponse.ScrollID,
	}, nil
}

// NextScroll returns next batch of results. New scroll id can be returned.
// When scroll is completed [StopScroll] to free up resources otherwise resources.
func NextScroll[T any](ctx context.Context, cl *opensearch.Client, scrollID string, scrollWindow time.Duration) (*ScrollResponse[T], error) {
	var osResponse opensearchapi.ScrollGetResp
	resp, err := cl.Do(ctx, opensearchapi.ScrollGetReq{
		ScrollID: scrollID,
		Params: opensearchapi.ScrollGetParams{
			Scroll: scrollWindow,
		},
	}, &osResponse)
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, osError(resp)
	}

	total := osResponse.Hits.Total.Value

	docs, err := mapDocs[T](osResponse.Hits.Hits)
	if err != nil {
		return nil, err
	}

	if osResponse.ScrollID == nil {
		return nil, ErrScrollNotDefined
	}

	return &ScrollResponse[T]{
		Docs:     docs,
		ScrollID: *osResponse.ScrollID,
		Total:    total,
	}, nil

}

// StopScroll frees up resources tied up to given scroll.
func StopScroll(ctx context.Context, cl *opensearch.Client, scrollID string) error {
	var osResponse opensearchapi.ScrollDeleteResp
	resp, err := cl.Do(ctx, opensearchapi.ScrollDeleteReq{
		ScrollIDs: []string{scrollID},
	}, &osResponse)
	if err != nil {
		return err
	}
	if resp.IsError() {
		return osError(resp)
	}

	if !osResponse.Succeeded {
		return serr.Wrap("", ErrScrollDeleteFailed, serr.String("scrollID", scrollID))
	}

	if osResponse.NumFreed != 1 {
		return serr.Wrap("", ErrScrollNotFreed, serr.String("scrollID", scrollID))
	}

	return nil
}

func osError(resp *opensearch.Response) error {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return serr.Wrap("search failed", ErrOpensearchRequestFailed, serr.Int("statusCode", resp.StatusCode), serr.String("body", string(b)))
}

func mapDocs[T any](hits []opensearchapi.SearchHit) ([]IDedDocument[T], error) {
	docs := make([]IDedDocument[T], 0, len(hits))
	for _, h := range hits {
		var doc T
		if err := json.Unmarshal(h.Source, &doc); err != nil {
			return nil, err
		}
		docs = append(docs, IDedDocument[T]{
			ID:       h.ID,
			Document: &doc,
		})
	}

	return docs, nil
}

func buildQuery(expr Expr, orderBy string, pag *Pagination) (*searchQuery, error) {
	maps, err := expr.Map(OpenSearch)
	if err != nil {
		return nil, err
	}
	if m, ok := maps.(Map); ok {
		maps = []Map{m} // the "must" attribute shall be an array
	}
	arr, ok := maps.([]Map)
	if !ok {
		return nil, fmt.Errorf("%w %T", ErrOpensearchBadRequest, maps)
	}

	q := searchQuery{
		Query: searchBool{
			Bool: searchMust{
				Must: arr,
			},
		},
		Sort: nil,
		From: nil,
		Size: nil,
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

	return &q, nil
}

// Pagination contains the offset and limit for searches.
type Pagination struct {
	From int
	Size int
}

// Convertor provides a [Convert] method to convert the document into a domain object.
type Convertor[T any] interface {
	Convert(string) (*T, error)
}

// IDedDocument is an IDed document.
type IDedDocument[T any] struct {
	ID       string
	Document *T
}

// Represents scroll response.
type ScrollResponse[T any] struct {
	Docs     []IDedDocument[T]
	ScrollID string
	Total    int
}

// Convert converts a document conforming to [Convertor] into a domain object.
func Convert[T any, D any, PD interface {
	pointer.Pointer[D]
	Convertor[T]
}](doc IDedDocument[D]) (*T, error) {
	return PD(doc.Document).Convert(doc.ID)
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

// UpdateOption allows customization of Update behavior.
type UpdateOption func(*opensearchapi.UpdateParams)

// WithRefresh sets the refresh flag to "true".
// https://opensearch.org/docs/latest/api-reference/document-apis/update-document/#query-parameters
func WithRefresh() UpdateOption {
	return func(p *opensearchapi.UpdateParams) {
		p.Refresh = "true"
	}
}

// WithRetryOnConflict sets how many times the update should be retried on conflict.
func WithRetryOnConflict(n int) UpdateOption {
	return func(p *opensearchapi.UpdateParams) {
		p.RetryOnConflict = &n
	}
}
