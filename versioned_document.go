package search

// VersionedDocument is document which's version is controlled externally.
// If document implements [VersionedDocument]. Then document version and version type, will
// be applied automatically. This is useful if documents, are indexed in parallel.
type VersionedDocument interface {
	Version() int
	VersionType() VersionType
}
