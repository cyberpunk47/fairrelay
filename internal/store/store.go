package store

import (
	"context"
	"errors"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)

type Store interface {
	InsertEvent(ctx context.Context, e domain.NewEvent) (id int64, created bool, err error)
}

var ErrUnknownEndpoint = errors.New("endpoint not found for this tenant")
