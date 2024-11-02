package db

import (
	"fmt"

	"github.com/otto8-ai/kinm/pkg/db/errors"
	"github.com/otto8-ai/kinm/pkg/db/errtypes"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func translateDuplicateEntryErr(err error, gvk schema.GroupVersionKind, objName string) error {
	if errtypes.IsUniqueConstraintErr(err) {
		return errors.NewConflict(gvk, objName, fmt.Errorf("object has been modified, please apply your changes to the latest version and try again"))
	}
	return err
}

func newPartitionRequiredError() error {
	return apierrors.NewInternalError(fmt.Errorf("partition ID required"))
}
