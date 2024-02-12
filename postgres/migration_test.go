package postgres

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"testing"
)

func Test_CreateTable(t *testing.T) {
	f, err := os.Open("../test/test_create_table.yml")
	y, err := io.ReadAll(f)
	m := &Migration{}
	err = yaml.Unmarshal(y, m)

	psqlconn := fmt.Sprintf("host=localhost port=5432 user=docker password=docker dbname=docker sslmode=disable")
	db, err := sql.Open("postgres", psqlconn)
	defer db.Close()

	s := New(db)
	err = s.Migrate(m)
	assert.Nil(t, err)
}
