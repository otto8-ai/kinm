package stores

import (
	"github.com/acorn-io/kinm/pkg/strategy"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Creater  = (*CreateOnlyStore)(nil)
	_ strategy.Base = (*CreateOnlyStore)(nil)
)

type CreateOnlyStore struct {
	*strategy.SingularNameAdapter
	*strategy.CreateAdapter
	*strategy.DestroyAdapter
	*strategy.TableAdapter
}
