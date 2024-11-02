package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/otto8-ai/kinm/pkg/db/glogrus"
	"github.com/otto8-ai/kinm/pkg/pg"
	"github.com/otto8-ai/kinm/pkg/strategy"
	"github.com/otto8-ai/kinm/pkg/types"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig"
	"k8s.io/apiserver/pkg/storage/value"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type Factory struct {
	DB                  *gorm.DB
	SQLDB               *sql.DB
	Postgres            bool
	schema              *runtime.Scheme
	migrationTimeout    time.Duration
	AutoMigrate         bool
	transformers        map[schema.GroupKind]value.Transformer
	partitionIDRequired bool
}

type FactoryOption func(*Factory)

// WithMigrationTimeout sets a timeout for the initial database migration if auto migration is enabled.
func WithMigrationTimeout(timeout time.Duration) FactoryOption {
	return func(f *Factory) {
		f.migrationTimeout = timeout
	}
}

func WithEncryptionConfiguration(ctx context.Context, configPath string, apiServerId string) (FactoryOption, error) {
	encryptionConf, err := encryptionconfig.LoadEncryptionConfig(ctx, configPath, false, apiServerId)
	if err != nil {
		return nil, err
	}

	// Although the config reading code expects GroupResources, we expect GroupKinds,
	// so just convert, assuming that the Resource is actually a Kind, but lowercase.
	transformers := make(map[schema.GroupKind]value.Transformer, len(encryptionConf.Transformers))
	for gr, t := range encryptionConf.Transformers {
		if len(gr.Resource) < 2 {
			return nil, fmt.Errorf("invalid Resource: %s", gr.Resource)
		}

		transformers[schema.GroupKind{Group: gr.Group, Kind: strings.ToUpper(gr.Resource[:1]) + gr.Resource[1:]}] = t
	}

	return func(f *Factory) {
		f.transformers = transformers
	}, nil
}

// WithPartitionIDRequired will configure the all DB strategies created from this factory to require a partition ID when querying the database.
func WithPartitionIDRequired() FactoryOption {
	return func(f *Factory) {
		f.partitionIDRequired = true
	}
}

func NewFactory(schema *runtime.Scheme, dsn string, opts ...FactoryOption) (*Factory, error) {
	f := &Factory{
		AutoMigrate: true,
		schema:      schema,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(f)
		}
	}

	var (
		gdb                    gorm.Dialector
		pool                   bool
		pg                     bool
		skipDefaultTransaction bool
	)
	if strings.HasPrefix(dsn, "sqlite://") {
		skipDefaultTransaction = true
		gdb = sqlite.Open(strings.TrimPrefix(dsn, "sqlite://"))
	} else if strings.HasPrefix(dsn, "postgres://") {
		gdb = postgres.Open(dsn)
		pg = true
		pool = true
	} else if strings.HasPrefix(dsn, "postgresql://") {
		gdb = postgres.Open(strings.Replace(dsn, "postgresql://", "postgres://", 1))
		pg = true
		pool = true
	} else {
		dsn = strings.TrimPrefix(dsn, "mysql://")
		pool = true
		gdb = mysql.Open(dsn)
	}
	db, err := gorm.Open(gdb, &gorm.Config{
		SkipDefaultTransaction: skipDefaultTransaction,
		Logger: glogrus.New(glogrus.Config{
			SlowThreshold:             200 * time.Millisecond,
			IgnoreRecordNotFoundError: true,
			LogSQL:                    true,
		}),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetConnMaxLifetime(time.Minute * 3)
	if pool {
		sqlDB.SetMaxIdleConns(5)
		sqlDB.SetMaxOpenConns(5)
	} else {
		sqlDB.SetMaxIdleConns(1)
		sqlDB.SetMaxOpenConns(1)
	}
	f.DB = db
	f.SQLDB = sqlDB
	f.Postgres = pg
	return f, nil
}

func (f *Factory) Scheme() *runtime.Scheme {
	return f.schema
}

func (f *Factory) Name() string {
	return "Mink DB"
}

func (f *Factory) Check(req *http.Request) error {
	err := f.SQLDB.PingContext(req.Context())
	if err != nil {
		logrus.Warnf("Failed to ping database: %v", err)
	}

	return err
}

type TableNamer interface {
	TableName() string
}

func (f *Factory) NewDBStrategy(obj types.Object) (strategy.CompleteStrategy, error) {
	gvk, err := apiutil.GVKForObject(obj, f.schema)
	if err != nil {
		return nil, err
	}

	tableName := strings.ToLower(gvk.Kind)
	if tn, ok := obj.(TableNamer); ok {
		tableName = tn.TableName()
	}

	if f.AutoMigrate && !f.Postgres {
		ctx := context.Background()
		if f.migrationTimeout != 0 {
			// If configured, set a timeout for the migration
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, f.migrationTimeout)
			defer cancel()
		}

		if err := f.DB.WithContext(ctx).Table(tableName).AutoMigrate(&Record{}); err != nil {
			return nil, err
		}
	}

	if f.Postgres {
		ctx := context.Background()
		if f.migrationTimeout != 0 {
			// If configured, set a timeout for the migration
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, f.migrationTimeout)
			defer cancel()
		}
		return pg.New(ctx, f.SQLDB, gvk, f.schema, tableName)
	}

	return NewStrategy(f.schema, obj, tableName, f.DB, f.transformers, f.partitionIDRequired)
}
