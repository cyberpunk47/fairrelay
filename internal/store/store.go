package store

import (
	"context"
	"errors"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)
type Store interface {
	InsertEvent(ctx context.Context , e domain.NewEvent) (id int64 ,created bool, err error)
}
// ErrUnknownEndpoint: the endpoint doesn't exist or belongs to another tenant.
var ErrUnknownEndpoint = errors.New("endpoint not found for this tenant")