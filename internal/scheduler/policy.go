package scheduler

import (
	"context"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)

type Policy interface {
	Next(ctx context.Context) ([]domain.ClaimedEvent, error)
	Name() string
}
