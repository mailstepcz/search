package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/mailstepcz/serr"
)

const (
	// aliasKey is the json field and log attribute name for an alias.
	aliasKey = "alias"
	// indexKey is the json field and log attribute name for an index.
	indexKey = "index"
)

var (
	// ErrAliasNotFound alias not found error.
	ErrAliasNotFound = errors.New("alias not found")
)

// Alias represents openSearch alias.
type Alias struct {
	Name  string
	Index string
}

type mapAny map[string]any
type aliasesBody struct {
	Actions []mapAny `json:"actions"`
}

// AliasSet sets alias to the index.
func AliasSet(ctx context.Context, client *opensearchapi.Client, index, alias string) error {
	body := aliasesBody{
		Actions: []mapAny{
			{"add": mapAny{indexKey: index, aliasKey: alias}},
		}}

	b, err := json.Marshal(body)
	if err != nil {
		return serr.Wrap("json marshalling aliases request", err)
	}

	aliasesReq := opensearchapi.AliasesReq{}
	aliasesReq.Body = bytes.NewReader(b)
	resp, err := client.Aliases(ctx, aliasesReq)
	if err != nil {
		return serr.Wrap("setting alias", err, serr.String(aliasKey, alias), serr.String(indexKey, index))
	}

	if !resp.Acknowledged {
		return serr.New("alias not acknowledged", serr.String(aliasKey, alias), serr.String(indexKey, index))
	}

	return nil
}

// AliasGet returns alias if exists.
func AliasGet(ctx context.Context, client *opensearchapi.Client, alias string) (*Alias, error) {
	// the alias is named as the index too, so a role limited to the alias's indices may read it, unlike _cat/aliases
	resp, err := client.Indices.Alias.Get(ctx, opensearchapi.AliasGetReq{
		Indices: []string{alias},
		Alias:   []string{alias},
	})
	if err != nil {
		if resp != nil && resp.Inspect().Response != nil && resp.Inspect().Response.StatusCode == http.StatusNotFound {
			return nil, serr.Wrap("alias not found", ErrAliasNotFound, serr.String("aliasName", alias))
		}
		return nil, err
	}

	indices := slices.Sorted(maps.Keys(resp.GetIndices()))
	if len(indices) > 1 {
		return nil, serr.New("unexpected index count for alias", serr.String("aliasName", alias), serr.Any("indices", indices))
	}

	if len(indices) == 0 {
		return nil, serr.Wrap("alias not found", ErrAliasNotFound, serr.String("aliasName", alias))
	}

	return &Alias{
		Name:  alias,
		Index: indices[0],
	}, nil
}

// AliasExists checks if any index has given alias.
func AliasExists(ctx context.Context, client *opensearchapi.Client, alias string) (bool, error) {
	osAlias, err := AliasGet(ctx, client, alias)
	if err != nil && !errors.Is(err, ErrAliasNotFound) {
		return false, serr.Wrap("getting alias", err, serr.String(aliasKey, alias))
	}

	return osAlias != nil, nil
}

// AliasSwitch removes existing alias and adds alias to new index.
func AliasSwitch(ctx context.Context, client *opensearchapi.Client, alias, index string) error {
	osAlias, err := AliasGet(ctx, client, alias)
	if err != nil {
		return serr.Wrap("getting alias", err, serr.String(aliasKey, alias))
	}

	body := aliasesBody{
		Actions: []mapAny{
			{"add": mapAny{indexKey: index, aliasKey: alias}},
			{"remove": mapAny{"index": osAlias.Index, aliasKey: alias}},
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		return serr.Wrap("json marshalling aliases request", err)
	}

	resp, err := client.Aliases(ctx, opensearchapi.AliasesReq{Body: bytes.NewReader(b)})
	if err != nil {
		return serr.Wrap("switching alias", err, serr.String(aliasKey, alias), serr.String(indexKey, index))
	}

	if !resp.Acknowledged {
		return serr.New("alias switch not acknowledged", serr.String(aliasKey, alias), serr.String(indexKey, index), serr.String("oldIndex", osAlias.Index))
	}

	return nil
}
