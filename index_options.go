package search

import "github.com/opensearch-project/opensearch-go/v4/opensearchapi"

type IndexParam func(params *opensearchapi.IndexParams)

// WithIndexParamVersion allows to set version of indexed document.
func WithIndexParamVersion(v int) IndexParam {
	return func(params *opensearchapi.IndexParams) {
		params.Version = &v
	}
}

// WithIndexParamVersionType allows to set refresh type of index params.
func WithIndexParamRefresh(refreshType RefreshType) IndexParam {
	return func(params *opensearchapi.IndexParams) {
		params.Refresh = string(refreshType)
	}
}

// WithIndexParamVersionType allows to set version type of index params.
func WithIndexParamVersionType(versionType VersionType) IndexParam {
	return func(params *opensearchapi.IndexParams) {
		params.VersionType = string(versionType)
	}
}

// RefreshType enum of allowed refresh types.
type RefreshType string

const (
	RefreshTypeTrue    RefreshType = "true"
	RefreshTypeFalse   RefreshType = "false"
	RefreshTypeWaitFor RefreshType = "wait_for"
)

// VersionType enum of allowed version types.
type VersionType string

const (
	VersionTypeExternal    VersionType = "external"
	VersionTypeExternalGTE VersionType = "external_gte"
)
