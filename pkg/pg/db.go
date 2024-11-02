package pg

import (
	"context"
	"database/sql"
	_ "embed"

	errors2 "github.com/otto8-ai/kinm/pkg/db/errors"
	"github.com/otto8-ai/kinm/pkg/pg/statements"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type db struct {
	sqlDB *sql.DB
	stmt  *statements.Statements
	gvk   schema.GroupVersionKind
}

func (d *db) Close() {
	_ = d.sqlDB.Close()
}

func (d *db) migrate(ctx context.Context) error {
	_, err := d.execContext(ctx, d.stmt.CreateSQL())
	return err
}

type txKey struct{}

func (d *db) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	if ok {
		return tx.ExecContext(ctx, query, args...)
	}
	return d.sqlDB.ExecContext(ctx, query, args...)
}

func (d *db) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	if ok {
		return tx.QueryContext(ctx, query, args...)
	}
	return d.sqlDB.QueryContext(ctx, query, args...)
}

func (d *db) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	if ok {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return d.sqlDB.QueryRowContext(ctx, query, args...)
}

type tx interface {
	Rollback() error
	Commit() error
}

type noopTx struct{}

func (n noopTx) Rollback() error {
	return nil
}

func (n noopTx) Commit() error {
	return nil
}

func (d *db) beginTx(ctx context.Context, options *sql.TxOptions) (context.Context, tx, error) {
	_, ok := ctx.Value(txKey{}).(*sql.Tx)
	if ok {
		// don't actually nest transactions
		return ctx, noopTx{}, nil
	}
	tx, err := d.sqlDB.BeginTx(ctx, options)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, txKey{}, tx), tx, nil
}

func (d *db) get(ctx context.Context, namespace, name string) (*record, error) {
	_, records, err := d.list(ctx, getNamespace(namespace), &name, 0, false, 0, 1)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, errors2.NewNotFound(d.gvk, name)
	}
	return &records[0], nil
}

type tableMeta struct {
	ListID       int64
	CompactionID int64
}

// list after=true will return all records after rev, whereas after=false it will return just the latest resourceVersion
// for each name,namespace pair for all records <= rev
func (d *db) list(ctx context.Context, namespace, name *string, rev int64, after bool, cont, limit int64) (tableMeta, []record, error) {
	if cont > 0 && rev <= 0 {
		panic("rev must be set when cont is set")
	}
	if after && cont != 0 {
		panic("cont must be zero when after is true")
	}

	ctx, tx, err := d.beginTx(ctx, &sql.TxOptions{
		// Repeatable read is needed to ensure that the ListID is consistent across multiple queries
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return tableMeta{}, nil, err
	}
	defer tx.Rollback()

	meta, records, err := d.doList(ctx, namespace, name, rev, after, cont, limit)
	if err != nil {
		return tableMeta{}, nil, err
	}

	if rev > 0 && !after {
		// Set the ListID to the requested revision
		meta.ListID = rev
	}

	// this can possibly be zero if when no results were found. Also notice the isolation is repeatable read
	// so that we will get the same ID that was used in the first query
	if meta.ListID == 0 {
		meta, err = d.getTableMeta(ctx)
		if err != nil {
			return tableMeta{}, nil, err
		}
	}

	// ListID can be zero if no records exist in the table. Also don't check if rev is zero that means
	// a specific revision was not requested and there we don't need to consider compaction. This condition
	// is important for when the compaction ID is greater than any existing ID in the table. That can happen
	// after a compaction where the last row was a delete=true row.
	if rev != 0 && meta.ListID != 0 && meta.ListID < meta.CompactionID {
		return meta, nil, errors2.NewCompactionError(uint(meta.ListID), uint(meta.CompactionID))
	}

	return meta, records, tx.Commit()
}

func (d *db) getTableMeta(ctx context.Context) (meta tableMeta, _ error) {
	err := d.queryRowContext(ctx, d.stmt.TableMetaSQL()).Scan(&meta.ListID, &meta.CompactionID)
	return meta, err
}

func (d *db) doList(ctx context.Context, namespace, name *string, rev int64, after bool, cont, limit int64) (meta tableMeta, _ []record, _ error) {
	var (
		rows *sql.Rows
		err  error
	)
	if after {
		rows, err = d.queryContext(ctx, d.stmt.ListAfterSQL(limit), namespace, name, rev)
	} else {
		rows, err = d.queryContext(ctx, d.stmt.ListSQL(limit), namespace, name, rev, cont)
	}
	if err != nil {
		return meta, nil, err
	}
	defer rows.Close()

	var records []record
	for rows.Next() {
		var (
			r       record
			created sql.NullBool
		)
		if err := rows.Scan(
			&meta.ListID,
			&meta.CompactionID,
			&r.id, &r.name, &r.namespace, &r.previousID, &r.uid, &created, &r.deleted, &r.value); err != nil {
			return meta, nil, err
		}
		if created.Valid {
			r.created = created.Bool
		}
		records = append(records, r)
	}
	return meta, records, nil
}

func (d *db) insert(ctx context.Context, rec record) (id int64, _ error) {
	ctx, tx, err := d.beginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	id, err = d.doInsert(ctx, rec)
	if err != nil {
		return 0, err
	}

	return id, tx.Commit()
}

type sqlError interface {
	SQLState() string
}

func (d *db) doInsert(ctx context.Context, rec record) (id int64, _ error) {
	_, err := d.execContext(ctx, d.stmt.TableLockSQL())
	if err != nil {
		return 0, err
	}

	if rec.id != 0 {
		panic("id must be zero")
	}
	if rec.created && rec.previousID != nil {
		panic("previousID must be nil when created is true")
	}
	if !rec.created && rec.previousID == nil {
		panic("previousID must be set when created is false")
	}

	// only check on update, on create DB constraints errors
	if !rec.created {
		existing, err := d.get(ctx, rec.namespace, rec.name)
		if apierrors.IsNotFound(err) {
			if !rec.created {
				return 0, errors2.NewResourceVersionMismatch(d.gvk, rec.name)
			}
		} else if err != nil {
			return 0, err
		} else if existing.id != *rec.previousID {
			return 0, errors2.NewResourceVersionMismatch(d.gvk, rec.name)
		} else if existing.uid != rec.uid {
			return 0, errors2.NewUIDMismatch(rec.name, existing.uid, rec.uid)
		}
	}

	var createdAny any
	if rec.created {
		createdAny = true
	}
	err = d.queryRowContext(ctx, d.stmt.InsertSQL(),
		rec.name,
		rec.namespace,
		rec.previousID,
		rec.uid,
		createdAny,
		rec.deleted,
		rec.value).Scan(&id)
	if pgErr, ok := err.(sqlError); ok && pgErr.SQLState() == "23505" {
		return 0, errors2.NewAlreadyExists(d.gvk, rec.name)
	} else if err != nil {
		return 0, err
	}
	return
}

func (d *db) delete(ctx context.Context, r record) (int64, error) {
	ctx, tx, err := d.beginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if r.previousID == nil || *r.previousID <= 0 {
		panic("previousID must be set and greater than zero")
	}

	r.created = false
	r.deleted = true

	id, err := d.doInsert(ctx, r)
	if err != nil {
		return 0, err
	}

	if _, err := d.execContext(ctx, d.stmt.ClearCreatedSQL(), r.namespace, r.name, id); err != nil {
		return 0, err
	}

	return id, tx.Commit()
}

func (d *db) compact(ctx context.Context) (resultCount int64, _ error) {
	_, err := d.execContext(ctx, d.stmt.UpdateCompactionSQL())
	if err != nil {
		return 0, err
	}

	for {
		result, err := d.execContext(ctx, d.stmt.CompactSQL())
		if err != nil {
			return resultCount, err
		}
		count, err := result.RowsAffected()
		resultCount += count
		if err != nil || count == 0 {
			return resultCount, err
		}
	}
}
