package reactive

import (
	"context"
	"time"

	"github.com/samsarahq/thunder/thunderpb"
)

func InvalidateAfter(ctx context.Context, d time.Duration) {
	r := NewResource()
	timer := time.AfterFunc(d, r.Invalidate)
	r.Cleanup(func() { timer.Stop() })
	AddDependency(ctx, r, &thunderpb.ExpirationTime{Time: time.Now().Add(d)})
}

func InvalidateAt(ctx context.Context, t time.Time) {
	InvalidateAfter(ctx, t.Sub(time.Now()))
}
