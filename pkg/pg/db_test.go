package pg

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"

	_ "github.com/lib/pq"
	"github.com/otto8-ai/kinm/pkg/pg/statements"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "knowledge"
	password = "knowledge"
	dbname   = "knowledge"
)

func newDatabase(t *testing.T) *db {
	t.Helper()
	sqldb := newSQLDB(t)
	_, err := sqldb.ExecContext(context.Background(), "DROP TABLE IF EXISTS recordstest")
	require.NoError(t, err)
	s := &db{
		db:   sqldb,
		stmt: statements.New("recordstest"),
		gvk:  testGVK,
	}
	require.NoError(t, s.migrate(context.Background()))
	insertRows(t, s)
	_, err = sqldb.Exec("INSERT INTO compaction(name, id) values('recordstest', 1) ON CONFLICT(name) DO UPDATE SET id = 1")
	require.NoError(t, err)
	return s
}

func newSQLDB(t *testing.T) *sql.DB {
	t.Helper()

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func TestMigrate(t *testing.T) {
	_ = newDatabase(t)
}

func insertRows(t *testing.T, s *db) {
	t.Helper()

	id, err := s.insert(context.Background(), record{
		name:      "test",
		namespace: "default",
		created:   true,
		value:     "value1",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), id)

	id, err = s.insert(context.Background(), record{
		name:       "test",
		namespace:  "default",
		previousID: &id,
		value:      "value2",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(2), id)

	id, err = s.insert(context.Background(), record{
		name:       "test",
		namespace:  "default",
		previousID: &id,
		value:      "value3",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(3), id)
}

func TestInsert(t *testing.T) {
	s := newDatabase(t)

	_, records, err := s.list(context.Background(), nil, nil, 1, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, true, records[0].created)

	_, records, err = s.list(context.Background(), nil, nil, 1, true, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 2)

	assert.Equal(t, false, records[0].created)
	assert.Equal(t, false, records[1].created)
}

func TestAlreadyExists(t *testing.T) {
	s := newDatabase(t)

	_, err := s.insert(context.Background(), record{
		name:      "test",
		namespace: "default",
		created:   true,
		value:     "value1",
	})
	require.Error(t, err)
	assert.True(t, apierrors.IsAlreadyExists(err))
}

func TestGet(t *testing.T) {
	s := newDatabase(t)

	rec, err := s.get(context.Background(), "default", "test")
	require.NoError(t, err)
	assert.Equal(t, "test", rec.name)
	assert.Equal(t, "default", rec.namespace)
	assert.Equal(t, int64(3), rec.id)
	assert.Equal(t, int64(2), *rec.previousID)
}

func TestGetNotFound(t *testing.T) {
	s := newDatabase(t)

	_, err := s.get(context.Background(), "not_found", "default")
	assert.True(t, apierrors.IsNotFound(err))

	_, err = s.get(context.Background(), "deleted", "default")
	assert.True(t, apierrors.IsNotFound(err))
}

func ptr[T any](v T) *T {
	return &v
}

func TestCompactionError(t *testing.T) {
	s := newDatabase(t)

	meta, records, err := s.list(context.Background(), ptr("default"), nil, 0, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	_, err = s.db.Exec("UPDATE compaction SET id = 3 WHERE name = 'recordstest'")
	require.NoError(t, err)

	meta, records, err = s.list(context.Background(), ptr("default"), nil, 0, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(3), meta.CompactionID)

	_, _, err = s.list(context.Background(), ptr("default"), nil, 2, false, 0, 0)
	assert.True(t, apierrors.IsResourceExpired(err))
}

func TestList(t *testing.T) {
	s := newDatabase(t)

	meta, records, err := s.list(context.Background(), ptr("default"), nil, 0, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), ptr("not_default"), nil, 0, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 0)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), nil, nil, 0, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), records[0].id)
	assert.Equal(t, "value3", records[0].value)
	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), nil, nil, 2, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(2), records[0].id)
	assert.Equal(t, "value2", records[0].value)
	assert.Equal(t, int64(2), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)
}

func TestListAfter(t *testing.T) {
	s := newDatabase(t)

	meta, records, err := s.list(context.Background(), ptr("default"), nil, 1, true, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 2)

	assert.Equal(t, "value2", records[0].value)
	assert.Equal(t, "value3", records[1].value)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)
}

func TestDelete(t *testing.T) {
	s := newDatabase(t)

	r, err := s.get(context.Background(), "default", "test")
	require.NoError(t, err)

	id := r.id
	r.previousID = &id
	r.id = 0

	id, err = s.delete(context.Background(), *r)
	require.NoError(t, err)

	assert.Equal(t, int64(4), id)

	_, records, err := s.list(context.Background(), ptr("default"), ptr("test"), 0, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 0)

	_, err = s.get(context.Background(), "default", "test")
	assert.True(t, apierrors.IsNotFound(err))

	_, err = s.db.ExecContext(context.Background(), "DELETE FROM compaction WHERE name = 'recordstest'")
	require.NoError(t, err)

	_, records, err = s.list(context.Background(), &r.namespace, &r.name, id-1, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 1)
	assert.False(t, records[0].created)

	_, records, err = s.list(context.Background(), &r.namespace, &r.name, 1, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 1)
	assert.True(t, records[0].created)

	_, records, err = s.list(context.Background(), &r.namespace, &r.name, 1, true, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 3)

	assert.Equal(t, "value2", records[0].value)
	assert.False(t, records[0].deleted)

	assert.Equal(t, "value3", records[1].value)
	assert.False(t, records[1].deleted)

	assert.Equal(t, "value3", records[2].value)
	assert.True(t, records[2].deleted)

	_, err = s.insert(context.Background(), record{
		name:      "test",
		namespace: "default",
		created:   true,
	})
	require.NoError(t, err)
}

func TestCompaction(t *testing.T) {
	s := newDatabase(t)

	test2ID, err := s.insert(context.Background(), record{
		name:    "test2",
		value:   "value1",
		created: true,
	})
	require.NoError(t, err)

	test3ID, err := s.insert(context.Background(), record{
		name:    "test3",
		value:   "value1",
		created: true,
	})
	require.NoError(t, err)

	_, err = s.insert(context.Background(), record{
		name:       "test2",
		value:      "value2",
		previousID: &test2ID,
	})
	require.NoError(t, err)

	test3ID, err = s.insert(context.Background(), record{
		name:       "test3",
		value:      "value1",
		previousID: &test3ID,
	})
	require.NoError(t, err)

	_, err = s.insert(context.Background(), record{
		name:       "test3",
		value:      "value1",
		deleted:    true,
		previousID: &test3ID,
	})
	require.NoError(t, err)

	_, records, err := s.list(context.Background(), nil, nil, 1, true, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 7)

	deleted, err := s.compact(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(6), deleted)

	var count int64
	err = s.db.QueryRow("SELECT count(*) FROM recordstest").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	_, records, err = s.list(context.Background(), nil, nil, 8, false, 0, 0)
	require.NoError(t, err)
	assert.Len(t, records, 2)

	assert.Equal(t, int64(3), records[0].id)
	assert.Equal(t, "test", records[0].name)
	assert.Equal(t, "value3", records[0].value)

	assert.Equal(t, int64(6), records[1].id)
	assert.Equal(t, "test2", records[1].name)
	assert.Equal(t, "value2", records[1].value)
}

func TestConflict(t *testing.T) {
	s := newDatabase(t)
	_, err := s.insert(context.Background(), record{
		name:       "test",
		namespace:  "default",
		previousID: ptr(int64(1)),
		value:      "value",
	})
	assert.True(t, apierrors.IsConflict(err))
}

func TestUIDMatch(t *testing.T) {
	s := newDatabase(t)
	_, err := s.insert(context.Background(), record{
		name:       "test",
		namespace:  "default",
		previousID: ptr(int64(3)),
		uid:        "uid",
		value:      "value",
	})
	require.NotNil(t, err)
	assert.Equal(t, "Operation cannot be fulfilled on TestKind.testgroup \"test\": UID mismatch existing  != new uid", err.Error())
	assert.True(t, apierrors.IsConflict(err))
}
