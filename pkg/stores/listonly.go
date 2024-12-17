package stores

import (
	"github.com/obot-platform/kinm/pkg/strategy"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Lister   = (*ListOnlyStore)(nil)
	_ strategy.Base = (*ListOnlyStore)(nil)
)

type ListOnlyStore struct {
	*strategy.SingularNameAdapter
	*strategy.ListAdapter
	*strategy.DestroyAdapter
	*strategy.NewAdapter
}

func (r *ListOnlyStore) NamespaceScoped() bool {
	return r.ListAdapter.NamespaceScoped()
}
