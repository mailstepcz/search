package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/mailstepcz/serr"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

var (
	// ErrBulkItemError fails if there was an error in the bulk response.
	ErrBulkItemError = errors.New("bulk item error")
)

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
func Bulk[T any](ctx context.Context, cl *opensearch.Client, docs []BulkOperation[T]) error {
	return bulk(ctx, cl, docs, nil)
}

// BulkWithRefresh sends a bulk request with the specified ops and refresh = true parameter.
// https://opensearch.org/docs/latest/api-reference/document-apis/bulk/#query-parameters
func BulkWithRefresh[T any](ctx context.Context, cl *opensearch.Client, docs []BulkOperation[T]) error {
	return bulk(ctx, cl, docs, &opensearchapi.BulkParams{Refresh: "true"})
}

func bulk[T any](ctx context.Context, cl *opensearch.Client, ops []BulkOperation[T], params *opensearchapi.BulkParams) error {
	b, err := buildBulkBody(ops)
	if err != nil {
		return err
	}

	req := opensearchapi.BulkReq{
		Body: bytes.NewReader(b),
	}

	if params != nil {
		req.Params = *params
	}

	var bulkResponse opensearchapi.BulkResp
	resp, err := cl.Do(ctx, req, &bulkResponse)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return osError(resp)
	}

	if bulkResponse.Errors {
		var errs error
		for _, items := range bulkResponse.Items {
			for _, item := range items {
				if item.Error == nil {
					continue
				}

				errors.Join(errs, serr.Wrap(
					"",
					ErrBulkItemError,
					serr.String("id", item.ID),
					serr.String("index", item.Index),
					serr.String("type", item.Error.Type),
					serr.String("reason", item.Error.Reason),
					serr.String("causeType", item.Error.Cause.Type),
					serr.String("causeReason", item.Error.Cause.Reason),
				))
			}
		}
		return errs
	}

	return nil
}

func buildBulkBody[T any](ops []BulkOperation[T]) ([]byte, error) {
	var buffer bytes.Buffer

	for _, op := range ops {
		metaBytes, err := json.Marshal(map[string]interface{}{
			string(op.OperationType): map[string]string{
				"_index": op.Index,
				"_id":    op.ID,
			},
		})
		if err != nil {
			return nil, serr.Wrap("marshalling meta JSON", err, serr.String("index", op.Index), serr.String("id", op.ID))
		}
		buffer.Write(metaBytes)
		buffer.WriteByte('\n')

		if op.OperationType != OpDelete && op.Doc != nil {
			docBytes, err := json.Marshal(op.Doc)
			if err != nil {
				return nil, serr.Wrap("marshalling document JSON", err, serr.String("operationType", string(op.OperationType)))
			}

			buffer.Write(docBytes)
			buffer.WriteByte('\n')
		}
	}

	return buffer.Bytes(), nil
}
