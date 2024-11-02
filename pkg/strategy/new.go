package strategy

import (
	"github.com/otto8-ai/kinm/pkg/types"
	"k8s.io/apimachinery/pkg/runtime"
)

type Newer interface {
	New() types.Object
}

type NewAdapter struct {
	n Newer
}

func (n *NewAdapter) New() runtime.Object {
	return n.n.New()
}

func NewNew(n Newer) *NewAdapter {
	return &NewAdapter{
		n: n,
	}
}
