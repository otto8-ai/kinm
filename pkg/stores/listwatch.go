package stores

import (
	"github.com/obot-platform/kinm/pkg/strategy"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Lister   = (*ListWatchStore)(nil)
	_ rest.Watcher  = (*ListWatchStore)(nil)
	_ strategy.Base = (*ListWatchStore)(nil)
)

type ListWatchStore struct {
	*strategy.SingularNameAdapter
	*strategy.NewAdapter
	*strategy.ListAdapter
	*strategy.WatchAdapter
	*strategy.DestroyAdapter
	*strategy.TableAdapter
}

func (r *ListWatchStore) NamespaceScoped() bool {
	return r.ListAdapter.NamespaceScoped()
}
