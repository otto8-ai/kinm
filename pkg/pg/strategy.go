package pg

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/otto8-ai/kinm/pkg/pg/statements"
	"github.com/otto8-ai/kinm/pkg/strategy"
	"github.com/otto8-ai/kinm/pkg/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

var _ strategy.CompleteStrategy = (*Strategy)(nil)

type Strategy struct {
	db              db
	objTemplate     types.Object
	objListTemplate types.ObjectList
	scheme          *runtime.Scheme

	broadcastLock sync.Mutex
	broadcast     chan struct{}
}

type record struct {
	id               int64
	name, namespace  string
	previousID       *int64
	uid              string
	created, deleted bool
	value            string
}

func (r *record) Unmarshal(obj types.Object) error {
	if err := json.Unmarshal([]byte(r.value), obj); err != nil {
		return err
	}
	obj.SetResourceVersion(strconv.FormatInt(r.id, 10))
	return nil
}

func New(ctx context.Context, sqlDB *sql.DB, gvk schema.GroupVersionKind, scheme *runtime.Scheme, tableName string) (*Strategy, error) {
	objTemplate, err := scheme.New(gvk)
	if err != nil {
		return nil, err
	}
	objListTemplate, err := scheme.New(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	if err != nil {
		return nil, err
	}
	newDB := db{
		sqlDB: sqlDB,
		stmt:  statements.New(tableName),
		gvk:   gvk,
	}
	if err := newDB.migrate(ctx); err != nil {
		return nil, err
	}

	return &Strategy{
		db:              newDB,
		objTemplate:     objTemplate.(types.Object),
		objListTemplate: objListTemplate.(types.ObjectList),
		scheme:          scheme,
		broadcast:       make(chan struct{}),
	}, nil
}

func (s *Strategy) Create(ctx context.Context, object types.Object) (types.Object, error) {
	if object.GetUID() == "" {
		return nil, fmt.Errorf("object must have a UID")
	}

	defer s.broadcastChange()

	// On create all objects have a generation of 1
	object.SetGeneration(1)

	var buf strings.Builder
	if err := json.NewEncoder(&buf).Encode(object); err != nil {
		return nil, err
	}

	id, err := s.db.insert(ctx, record{
		name:      object.GetName(),
		namespace: object.GetNamespace(),
		uid:       string(object.GetUID()),
		created:   true,
		value:     buf.String(),
	})
	if err != nil {
		return nil, err
	}

	result := object.DeepCopyObject().(types.Object)
	result.SetResourceVersion(strconv.FormatInt(id, 10))
	return result, nil
}

func (s *Strategy) New() types.Object {
	return s.objTemplate.DeepCopyObject().(types.Object)
}

func (s *Strategy) Get(ctx context.Context, namespace, name string) (types.Object, error) {
	rec, err := s.db.get(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	result := s.New()
	if err := json.Unmarshal([]byte(rec.value), result); err != nil {
		return nil, err
	}
	result.SetResourceVersion(strconv.FormatInt(rec.id, 10))
	return result, nil
}

func (s *Strategy) Update(ctx context.Context, obj types.Object) (types.Object, error) {
	defer s.broadcastChange()
	return s.doUpdate(ctx, obj, true, false)
}

func (s *Strategy) doUpdate(ctx context.Context, obj types.Object, updateGeneration, deleted bool) (types.Object, error) {
	var buf strings.Builder

	obj = obj.DeepCopyObject().(types.Object)
	if updateGeneration {
		obj.SetGeneration(obj.GetGeneration() + 1)
	}

	if err := json.NewEncoder(&buf).Encode(obj); err != nil {
		return nil, err
	}

	var (
		resourceVersion int64
		err             error
	)
	if obj.GetResourceVersion() != "" {
		resourceVersion, err = strconv.ParseInt(obj.GetResourceVersion(), 10, 64)
		if err != nil {
			return nil, err
		}
	}

	rec := record{
		name:       obj.GetName(),
		namespace:  obj.GetNamespace(),
		previousID: &resourceVersion,
		uid:        string(obj.GetUID()),
		value:      buf.String(),
	}

	var id int64
	if deleted {
		id, err = s.db.delete(ctx, rec)
	} else {
		id, err = s.db.insert(ctx, rec)
	}
	if err != nil {
		return nil, err
	}

	obj.SetResourceVersion(strconv.FormatInt(id, 10))
	return obj, nil
}

func (s *Strategy) UpdateStatus(ctx context.Context, obj types.Object) (types.Object, error) {
	return s.doUpdate(ctx, obj, false, false)
}

func (s *Strategy) prepareList(opts storage.ListOptions) (storage.ListOptions, error) {
	if opts.ResourceVersionMatch != "" {
		return opts, fmt.Errorf("resource version match is not supported")
	}

	if opts.Predicate.Continue != "" && opts.ResourceVersion == "" {
		return opts, fmt.Errorf("resource version is required with continue")
	}

	if opts.Predicate.Label == nil {
		opts.Predicate.Label = labels.Everything()
	}
	if opts.Predicate.Field == nil {
		opts.Predicate.Field = fields.Everything()
	}
	if opts.Predicate.GetAttrs == nil {
		opts.Predicate.GetAttrs = storage.DefaultNamespaceScopedAttr
	}

	return opts, nil
}

func (s *Strategy) List(ctx context.Context, namespace string, opts storage.ListOptions) (types.ObjectList, error) {
	var (
		objs       []runtime.Object
		listResult = s.NewList()
		err        error
	)

	opts, err = s.prepareList(opts)
	if err != nil {
		return nil, err
	}

	listResourceVersion, iter, err := newLister(ctx, &s.db, namespace, opts, false)
	if err != nil {
		return nil, err
	}

	for rec, err := range iter {
		if err != nil {
			return nil, err
		}

		obj := s.New()
		if err := rec.Unmarshal(obj); err != nil {
			return nil, err
		}

		if match, err := opts.Predicate.Matches(obj); err != nil {
			return nil, err
		} else if !match {
			continue
		}

		// We check this at the end because the next object could possibly not match the predicate so
		// we don't want to do continue token to them result in the next call being an empty list.
		if opts.Predicate.Limit > 0 && len(objs) >= int(opts.Predicate.Limit) {
			listResult.SetContinue(objs[len(objs)-1].(types.Object).GetResourceVersion())
			break
		}
		objs = append(objs, obj)
	}

	listResult.SetResourceVersion(listResourceVersion)
	return listResult, meta.SetList(listResult, objs)
}

func (s *Strategy) NewList() types.ObjectList {
	return s.objListTemplate.DeepCopyObject().(types.ObjectList)
}

func (s *Strategy) Delete(ctx context.Context, obj types.Object) (types.Object, error) {
	defer s.broadcastChange()
	return s.doUpdate(ctx, obj, false, true)
}

func (s *Strategy) Watch(ctx context.Context, namespace string, opts storage.ListOptions) (<-chan watch.Event, error) {
	opts, err := s.prepareList(opts)
	if err != nil {
		return nil, err
	}

	if opts.Predicate.Continue != "" {
		return nil, fmt.Errorf("continue is not supported in watch")
	}

	if opts.Predicate.Limit != 0 {
		return nil, fmt.Errorf("limit is not supported in watch")
	}

	if opts.ResourceVersion == "0" {
		opts.ResourceVersion = ""
	}

	// If resourceVersion is set we immediately go to watch phase and skip the historical list
	resourceVersion, lister, err := newLister(ctx, &s.db, namespace, opts, opts.ResourceVersion != "")
	if err != nil {
		return nil, err
	}

	opts.ResourceVersion = resourceVersion

	ch := make(chan watch.Event)
	go s.streamWatch(ctx, namespace, opts, lister, ch)
	return ch, nil
}

func toWatchEventError(err error) watch.Event {
	if _, ok := err.(apierrors.APIStatus); !ok {
		err = apierrors.NewInternalError(err)
	}
	status := err.(apierrors.APIStatus).Status()
	return watch.Event{
		Type:   watch.Error,
		Object: &status,
	}
}

func (s *Strategy) toWatchEvent(rec record) watch.Event {
	obj := s.New()
	if err := rec.Unmarshal(obj); err != nil {
		return toWatchEventError(err)
	}
	switch {
	case rec.created:
		return watch.Event{Type: watch.Added, Object: obj}
	case rec.deleted:
		return watch.Event{Type: watch.Deleted, Object: obj}
	default:
		return watch.Event{Type: watch.Modified, Object: obj}
	}
}

func (s *Strategy) broadcastChange() {
	s.broadcastLock.Lock()
	defer s.broadcastLock.Unlock()
	close(s.broadcast)
	s.broadcast = make(chan struct{})
	return
}

func (s *Strategy) waitChange() <-chan struct{} {
	s.broadcastLock.Lock()
	defer s.broadcastLock.Unlock()
	return s.broadcast
}

func (s *Strategy) streamWatch(ctx context.Context, namespace string, opts storage.ListOptions, lister iter.Seq2[record, error], ch chan watch.Event) {
	defer close(ch)

	var bookmarks <-chan time.Time
	if opts.ProgressNotify {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		bookmarks = ticker.C
	}

	for {
		for rec, err := range lister {
			if err != nil {
				ch <- toWatchEventError(err)
				return
			}
			ch <- s.toWatchEvent(rec)
		}

		var (
			newResourceVersion string
			err                error
		)

		newResourceVersion, lister, err = newLister(ctx, &s.db, namespace, opts, true)
		if err != nil {
			ch <- toWatchEventError(err)
			return
		}

		if newResourceVersion == opts.ResourceVersion {
			select {
			case <-ctx.Done():
				return
			case <-bookmarks:
				ch <- watch.Event{Type: watch.Bookmark, Object: nil}
			case <-s.waitChange():
			case <-time.After(2 * time.Second):
			}
		}

		opts.ResourceVersion = newResourceVersion
	}
}

func (s *Strategy) Destroy() {
	s.db.Close()
}

func (s *Strategy) Scheme() *runtime.Scheme {
	return s.scheme
}
