package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/mailstepcz/serr"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

var (
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
			{"add": mapAny{"index": index, "alias": alias}},
		}}

	b, err := json.Marshal(body)
	if err != nil {
		return serr.Wrap("json marshalling aliases request", err)
	}

	aliasesReq := opensearchapi.AliasesReq{}
	aliasesReq.Body = bytes.NewReader(b)
	resp, err := client.Aliases(ctx, aliasesReq)
	if err != nil {
		return serr.Wrap("setting alias", err, serr.String("alias", alias), serr.String("index", index))
	}

	if !resp.Acknowledged {
		return serr.New("alias not acknowledged", serr.String("alias", alias), serr.String("index", index))
	}

	return nil
}

// AliasGet returns alias if exists.
func AliasGet(ctx context.Context, client *opensearchapi.Client, alias string) (*Alias, error) {
	aliasGetReq := opensearchapi.CatAliasesReq{
		Aliases: []string{alias},
	}

	resp, err := client.Cat.Aliases(ctx, &aliasGetReq)
	if err != nil {
		return nil, err
	}

	if len(resp.Aliases) > 1 {
		return nil, serr.New("unexpected index count for alias", serr.String("aliasName", alias), serr.Any("indices", resp.Aliases))
	}

	if len(resp.Aliases) == 0 {
		return nil, serr.Wrap("alias not found", ErrAliasNotFound, serr.String("aliasName", alias))
	}

	return &Alias{
		Name:  alias,
		Index: resp.Aliases[0].Index,
	}, nil
}

// AliasExists checks if any index has given alias.
func AliasExists(ctx context.Context, client *opensearchapi.Client, alias string) (bool, error) {
	osAlias, err := AliasGet(ctx, client, alias)
	if err != nil && !errors.Is(err, ErrAliasNotFound) {
		return false, serr.Wrap("getting alias", err, serr.String("alias", alias))
	}

	return osAlias != nil, nil
}

// AliasSwitch removes existing alias and adds alias to new index. Returns real index which was pointing to alias.
func AliasSwitch(ctx context.Context, client *opensearchapi.Client, alias, index string) error {
	osAlias, err := AliasGet(ctx, client, alias)
	if err != nil {
		return serr.Wrap("getting alias", err, serr.String("alias", alias))
	}

	body := aliasesBody{
		Actions: []mapAny{
			{"add": mapAny{"index": index, "alias": alias}},
			{"remove": mapAny{"index": osAlias.Index, "alias": alias}},
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		return serr.Wrap("json marshalling aliases request", err)
	}

	resp, err := client.Aliases(ctx, opensearchapi.AliasesReq{Body: bytes.NewReader(b)})
	if err != nil {
		return serr.Wrap("switching alias", err, serr.String("alias", alias), serr.String("index", index))
	}

	if !resp.Acknowledged {
		return serr.New("alias switch not acknowledged", serr.String("alias", alias), serr.String("index", index), serr.String("oldIndex", osAlias.Index))
	}

	return nil
}
