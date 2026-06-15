package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildQueryResultWindow(t *testing.T) {
	expr := Eq[int]{Ident: "a", Value: 1234}

	tests := []struct {
		name    string
		pag     *Pagination
		wantErr bool
	}{
		{name: "nil pagination", pag: nil, wantErr: false},
		{name: "shallow window", pag: &Pagination{From: 0, Size: 10}, wantErr: false},
		{name: "at limit", pag: &Pagination{From: 9990, Size: 10}, wantErr: false},
		{name: "over limit", pag: &Pagination{From: 9990, Size: 20}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)

			_, err := buildQuery(expr, "", tt.pag)
			if tt.wantErr {
				req.Error(err)
				req.ErrorIs(err, ErrResultWindowExceeded)
			} else {
				req.NoError(err)
			}
		})
	}
}
