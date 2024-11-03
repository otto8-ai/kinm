package statements

import (
	"embed"
	_ "embed"
)

//go:embed *.sql
var fs embed.FS

func (s *Statements) CreateSQL() string           { return s.statements["migrate.sql"] }
func (s *Statements) InsertSQL() string           { return s.statements["insert.sql"] }
func (s *Statements) TableMetaSQL() string        { return s.statements["tablemeta.sql"] }
func (s *Statements) ClearCreatedSQL() string     { return s.statements["clearcreated.sql"] }
func (s *Statements) UpdateCompactionSQL() string { return s.statements["updatecompaction.sql"] }
func (s *Statements) CompactSQL() string          { return s.statements["compact.sql"] }
func (s *Statements) listSQL() string             { return s.statements["list.sql"] }
func (s *Statements) listAfterSQL() string        { return s.statements["listafter.sql"] }

func (s *Statements) TableLockSQL() string {
	if s.lock {
		return s.statements["tablelock.sql"]
	}
	return ""
}
