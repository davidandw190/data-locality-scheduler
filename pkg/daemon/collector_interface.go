package daemon

import (
	"context"
)

// NodeCollector defines the interface for all node capability collectors
type NodeCollector interface {
	Collect(ctx context.Context) (map[string]string, error)
	GetCapabilities() map[string]string
	Name() string
}
