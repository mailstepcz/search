package search

import (
	"context"
	"time"

	"github.com/opensearch-project/opensearch-go/v4"
)

// Scroller scrolls over the results.
type Scroller[T any] struct {
	docs         []IDedDocument[T]
	scrollID     string
	err          error
	total        int
	returned     int
	cl           *opensearch.Client
	scrollWindow time.Duration
	isClosed     bool
}

func newScroller[T any](response *ScrollResponse[T], cl *opensearch.Client, scrollWindow time.Duration) *Scroller[T] {
	return &Scroller[T]{
		docs:         response.Docs,
		scrollID:     response.ScrollID,
		total:        response.Total,
		err:          nil,
		cl:           cl,
		returned:     0,
		scrollWindow: scrollWindow,
		isClosed:     false,
	}
}

// Next returns true if there is at least one more doc for processing.
// Next should be called before the [Doc] function.
func (s *Scroller[T]) Next(ctx context.Context) bool {
	if s.returned == s.total || s.err != nil {
		return false
	}

	// there are still some docs in the stack.
	if len(s.docs) != 0 {
		return true
	}

	if err := s.loadNextScroll(ctx); err != nil {
		s.err = err
		return false
	}

	if len(s.docs) == 0 {
		return false
	}

	return true

}

// Doc returns next document for processing.
func (s *Scroller[T]) Doc() IDedDocument[T] {
	if len(s.docs) == 0 {
		panic("docs is an empty array, cannot access document from an empty array")
	}
	doc := s.docs[0]
	s.docs = s.docs[1:]
	s.returned++
	return doc
}

// Error returns an error if error has occurred during the scrolling.
func (s *Scroller[T]) Error() error {
	return s.err
}

// Close ends scroll and frees up resources tied up to given scroll.
// Close is safe to call multiple times.
func (s *Scroller[T]) Close(ctx context.Context) error {
	if s.isClosed {
		return nil
	}

	if err := StopScroll(ctx, s.cl, s.scrollID); err != nil {
		s.err = err
		return err
	}

	s.isClosed = true

	return nil
}

func (s *Scroller[T]) loadNextScroll(ctx context.Context) error {
	resp, err := NextScroll[T](ctx, s.cl, s.scrollID, s.scrollWindow)
	if err != nil {
		return err
	}

	s.docs = resp.Docs
	s.scrollID = resp.ScrollID

	return nil
}
