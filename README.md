# search
a Go wrapper for OpenSearch

## Errors

| Sentinel | When |
|----------|------|
| `ErrDocumentNotFound` | `Get`, `Delete*` or `Update*` of a document that does not exist, or a bulk `update` item whose document does not exist; a bulk `delete` of a missing document succeeds and is not in `BulkResult.Items` |
| `ErrIndexNotFound` | the index or alias a request addresses does not exist; `AliasGet` and `AliasSwitch` report a missing alias as `ErrAliasNotFound`, `AliasSwitch` and `AliasSwitchFrom` a missing old index as `ErrAliasMoved` |
| `ErrDocumentHasNewerVersion` | a version conflict of `Index`, `IndexWithRefresh`, `Update*` or a bulk item |
| `ErrBulkItemError` | a failed bulk item none of the sentinels above describes |
| `ErrOpensearchRequestFailed` | every error of a document or search request answered with an error status, the not-found and conflict ones included; bulk item errors do not carry it |

- Check the specific sentinel before `ErrOpensearchRequestFailed`, which is part of the not-found and conflict errors too.
- With `action.auto_create_index` enabled (the OpenSearch default), an index request creates a missing index and succeeds, and an update request creates it and returns `ErrDocumentNotFound`; neither returns `ErrIndexNotFound`. A delete never creates the index.
- Index and alias calls (`IndexCreate`, `IndexDelete`, `Alias*`) return the error of the opensearch-go client (e.g. an `*opensearch.StructError` when the body has an error object), not `ErrOpensearchRequestFailed`, except a missing alias, which `AliasGet` and `AliasSwitch` replace with `ErrAliasNotFound`; `IndexDelete`, `AliasSet`, `AliasSwitch` and `AliasSwitchFrom` add `ErrIndexNotFound` for a missing index.
