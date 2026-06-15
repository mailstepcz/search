package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/mailstepcz/serr"
)

var (
	// ErrBulkItemError fails if there was an error in the bulk response.
	ErrBulkItemError = errors.New("bulk item error")
)

// BulkItemResult holds the classified outcome of one non-success bulk item.
// Error wraps one of the package sentinel errors (ErrDocumentHasNewerVersion,
// ErrDocumentNotFound, ErrBulkItemError) so callers can branch with errors.Is.
type BulkItemResult struct {
	ID    string
	Error error
}

// BulkResult holds per-item outcomes for all non-success items in a bulk operation.
// Succeeded items are omitted. BulkResult is always non-nil when the returned error is nil.
type BulkResult struct {
	Items []BulkItemResult
}

// BulkOperationType represents the bulk operation type.
// https://opensearch.org/docs/latest/api-reference/document-apis/bulk/#request-body
type BulkOperationType string

// Bulk operations.
const (
	OpCreate BulkOperationType = "create"
	OpDelete BulkOperationType = "delete"
	OpIndex  BulkOperationType = "index"
	OpUpdate BulkOperationType = "update"
)

// BulkOperation for OpenSearch requests.
type BulkOperation[T any] struct {
	OperationType BulkOperationType `json:"-"`
	ID            string            `json:"id"`
	Index         string            `json:"index"`
	Doc           *T                `json:"doc"`
}

// Bulk sends a bulk request with the specified ops.
// Returns a *BulkResult with per-item outcomes for all non-success items; error is non-nil only on request-level failure.
func Bulk[T any](ctx context.Context, cl *opensearch.Client, docs []BulkOperation[T]) (*BulkResult, error) {
	return bulk(ctx, cl, docs, nil)
}

// BulkWithRefresh sends a bulk request with the specified ops and refresh = true parameter.
// Returns a *BulkResult with per-item outcomes for all non-success items; error is non-nil only on request-level failure.
// https://opensearch.org/docs/latest/api-reference/document-apis/bulk/#query-parameters
func BulkWithRefresh[T any](ctx context.Context, cl *opensearch.Client, docs []BulkOperation[T]) (*BulkResult, error) {
	return bulk(ctx, cl, docs, &opensearchapi.BulkParams{Refresh: "true"})
}

func bulk[T any](ctx context.Context, cl *opensearch.Client, ops []BulkOperation[T], params *opensearchapi.BulkParams) (*BulkResult, error) {
	var buf bytes.Buffer
	if err := buildBulkBody(ops, &buf); err != nil {
		return nil, err
	}

	req := opensearchapi.BulkReq{
		Body: &buf,
	}

	if params != nil {
		req.Params = *params
	}

	var bulkResponse opensearchapi.BulkResp
	resp, err := cl.Do(ctx, req, &bulkResponse)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() // nolint:errcheck

	if resp.IsError() {
		return nil, osError(resp)
	}

	var result BulkResult
	if bulkResponse.Errors && bulkResponse.Items != nil {
		for _, items := range bulkResponse.Items {
			for _, item := range items {
				if item.Error == nil {
					continue
				}

				sentinel := ErrBulkItemError
				switch item.Status {
				case http.StatusConflict:
					sentinel = ErrDocumentHasNewerVersion
				case http.StatusNotFound:
					sentinel = ErrDocumentNotFound
				}

				result.Items = append(result.Items, BulkItemResult{
					ID: item.ID,
					Error: serr.Wrap("bulk item failed", sentinel,
						serr.String("index", item.Index),
						serr.String("reason", item.Error.Reason),
					),
				})
			}
		}
	}

	return &result, nil
}

func buildBulkBody[T any](ops []BulkOperation[T], w io.Writer) error {
	encoder := json.NewEncoder(w)
	for _, op := range ops {
		meta := map[string]any{
			"_index": op.Index,
			"_id":    op.ID,
		}
		if op.OperationType != OpDelete && op.Doc != nil {
			if vd, ok := any(op.Doc).(VersionedDocument); ok {
				meta["version"] = vd.Version()
				meta["version_type"] = string(vd.VersionType())
			}
		}
		if err := encoder.Encode(map[string]any{
			string(op.OperationType): meta,
		}); err != nil {
			return serr.Wrap("marshalling meta JSON", err, serr.String("index", op.Index), serr.String("id", op.ID))
		}

		if op.OperationType != OpDelete && op.Doc != nil {
			if err := encoder.Encode(op.Doc); err != nil {
				return serr.Wrap("marshalling document JSON", err, serr.String("operationType", string(op.OperationType)))
			}
		}
	}

	return nil
}
