package db

import (
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/lightningsdk/core"
)

type Module struct {
	core.DefaultModule
	db *sqlx.DB
	*Config
}

func NewModule(app *core.App) core.Module {
	return &Module{}
}

func (m *Module) GetEmptyConfig() any {
	return &Config{}
}

func (m *Module) SetConfig(c any) {
	m.Config = c.(*Config)
}

func (m *Module) GetDB() (*sqlx.DB, error) {
	if m.db != nil {
		return m.db, nil
	}

	return sqlx.Open("postgres", "")
}

func From(app *core.App) (*sqlx.DB, error) {
	if db, ok := app.Modules["github.com/lightningsdk/db"]; ok {
		return db.(*Module).GetDB()
	}

	return nil, errors.New("db not configured")
}
