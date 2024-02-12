package postgres

import (
	"database/sql"
	"fmt"
	"strings"
)

type Column struct {
	Type     string `yaml:"type"`
	NotNull  bool   `yaml:"notnull"`
	Previous string `yaml:"previous"`
	Delete   bool   `yaml:"delete"`
}

type Index struct {
	Columns map[string]IndexColumn `yaml:"columns"`
	Delete  bool                   `yaml:"delete"`
}

type IndexColumn struct {
	Direction string `yaml:"direction"`
	Unique    string `yaml:"unique"`
}

type Table struct {
	Columns map[string]Column `yaml:"columns"`
	Indexes map[string]Index  `yaml:"indexes"`
	Delete  bool              `yaml:"delete"`
}

type Migration struct {
	Tables map[string]Table `yaml:"tables"`
}

type Service struct {
	schema string
	db     *sql.DB
}

func New(db *sql.DB) *Service {
	return &Service{
		schema: "public",
		db:     db,
	}
}

const (
	Noop = iota
	Create
	Update
	Delete
)

func (s *Service) Migrate(m *Migration) error {
	for t, td := range m.Tables {
		// make sure the table exists
		ex, err := s.tableExists(t)
		if err != nil {
			return err
		}
		if td.Delete && ex {
			err = s.deleteTable(t)
		} else if !ex {
			err = s.createTable(t, td)
		}
		if err != nil {
			return err
		}

		exf, err := s.getColumns(t)
		for c, cd := range td.Columns {
			switch s.getColumnAction(c, cd, exf) {
			case Noop:
			case Create:
				err = s.createColumn(t, c, cd)
			case Update:
				err = s.updateColumn(t, c, cd)
			case Delete:
				err = s.deleteColumn(t, c)
			}
			if err != nil {
				return err
			}
		}

		exi, err := s.getIndexes(t)
		for i, id := range td.Indexes {
			switch s.getIndexAction(i, id, exi) {
			case Noop:
			case Create:
				err = s.createIndex(t, i, id)
			case Update:
				err = s.updateIndex(t, i, id)
			case Delete:
				err = s.deleteIndex(t, i)
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) tableExists(t string) (bool, error) {
	res, err := s.db.Query("SELECT count(*) FROM information_schema.tables WHERE table_name = $1", t)
	if err != nil {
		return false, err
	}
	if !res.Next() {
		return false, nil
	}

	var c int
	err = res.Scan(&c)
	if err != nil {
		return false, nil
	}
	return c > 0, nil
}

func (s *Service) deleteTable(table string) error {
	_, err := s.db.Exec("")
	return err
}
func (s *Service) createTable(t string, td Table) error {
	// main table declaration
	query := fmt.Sprintf("CREATE TABLE %s.%s ", s.schema, t)
	vars := []any{}

	// add columns
	ps := []string{}
	for c, cd := range td.Columns {
		if cd.Delete {
			continue
		}
		cs := fmt.Sprintf("%s %s", c, cd.Type)
		if cd.NotNull {
			cs += " NOT NULL"
		}

		ps = append(ps, cs)
	}
	query += "(" + strings.Join(ps, ", ") + ")"

	_, err := s.db.Exec(query, vars...)
	return err
}

func (s *Service) getColumns(t string) (map[string]Column, error) {
	res, err := s.db.Query("SELECT column_name, column_default, is_nullable, data_type, character_maximum_length FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", s.schema, t)
	if err != nil {
		return nil, err
	}
	cs := map[string]Column{}
	for res.Next() {
		c := Column{}
		cn := ""
		if err := res.Scan(&cn, &c.Type); err != nil {
			return nil, err
		}
		cs[cn] = c
	}

	return cs, nil
}

func (s *Service) getColumnAction(f string, cd Column, exf map[string]Column) int {
	if fx, ok := exf[f]; ok {
		// the index exists
		if cd.Delete {
			return Delete
		} else if fx == cd {
			return Noop
		} else {
			return Update
		}
	} else {
		// index does not exist
		if cd.Delete {
			// should be deleted, doesn't exist, do nothing
			return Noop
		}
		// should exist but doesn't, create it
		return Create
	}
}

func (s *Service) createColumn(t, f string, cd Column) error {
	_, err := s.db.Exec("ALTER TABLE %s ADD COLUMN %s %s", t, f, cd.Type)
	return err
}

func (s *Service) updateColumn(t, f string, cd Column) error {
	_, err := s.db.Exec("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s", t, f, cd.Type)
	return err
}

func (s *Service) deleteColumn(t, f string) error {
	_, err := s.db.Exec("ALTER TABLE %s DROP COLUMN %s", t, f)
	return err
}

func (s *Service) getIndexes(t string) (map[string]string, error) {
	res, err := s.db.Query("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '%s'", t)
	is := map[string]string{}
	for res.Next() {
		var in, desc string
		if err = res.Scan(&in, &desc); err != nil {
			return nil, err
		}
		is[in] = desc
	}
	return is, err
}
func (s *Service) getIndexAction(i string, id Index, exi map[string]string) int {
	if ix, ok := exi[i]; ok {
		// the index exists
		if id.Delete {
			return Delete
		} else if ix == defineIndex(id) {
			return Noop
		} else {
			return Update
		}
	} else {
		// index does not exist
		if id.Delete {
			// should be deleted, doesn't exist, do nothing
			return Noop
		}
		// should exist but doesn't, create it
		return Create
	}
}
func (s *Service) createIndex(t, i string, id Index) error {
	query := "CREATE INDEX %s ON %s "
	vars := []any{i, t}

	// add columns
	ps := []string{}
	for c, cd := range id.Columns {
		if cd.Direction == "" {
			cd.Direction = "ASC"
		}
		vars = append(vars, c, cd.Direction)
		ps = append(ps, "%s %s")
	}
	query += "(" + strings.Join(ps, ", ") + ")"
	_, err := s.db.Exec(query, vars...)
	return err
}

func defineIndex(i Index) string {
	return ""
}

func (s *Service) updateIndex(t, i string, id Index) error {
	err := s.deleteIndex(t, i)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("DROP INDEX %s", i)
	return s.createIndex(t, i, id)
}
func (s *Service) deleteIndex(t, i string) error {
	_, err := s.db.Exec("DROP INDEX %s", i)
	return err
}
