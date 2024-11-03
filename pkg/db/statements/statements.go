package statements

import (
	_ "embed"
	"fmt"
	"strconv"
	"strings"
)

type Statements struct {
	tableName  string
	statements map[string]string
	lock       bool
}

func New(tableName string, lock bool) *Statements {
	s := &Statements{
		tableName:  tableName,
		statements: map[string]string{},
		lock:       lock,
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		panic("failed to read sql files: " + err.Error())
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		sql, err := fs.ReadFile(entry.Name())
		if err != nil {
			panic("failed to read sql file: " + err.Error())
		}
		s.initSQL(entry.Name(), sql)
	}
	return s
}

func (s *Statements) initSQL(name string, sqlData []byte) {
	// This is hacky, sue me
	sql := strings.ReplaceAll(string(sqlData), "'placeholder'", fmt.Sprintf(`'%s'`, s.tableName))
	sql = strings.ReplaceAll(sql, "placeholder", fmt.Sprintf(`"%s"`, s.tableName))
	sql = strings.ReplaceAll(sql, fmt.Sprintf(`"%s"_`, s.tableName), fmt.Sprintf(`%s_`, s.tableName))
	s.statements[name] = strings.TrimSpace(sql)
}

func (s *Statements) ListSQL(limit int64) string {
	if limit > 0 {
		return s.listSQL() + " LIMIT " + strconv.FormatInt(limit+1, 10)
	}
	return s.listSQL()
}

func (s *Statements) ListAfterSQL(limit int64) string {
	if limit > 0 {
		return s.listAfterSQL() + " LIMIT " + strconv.FormatInt(limit+1, 10)
	}
	return s.listAfterSQL()
}
