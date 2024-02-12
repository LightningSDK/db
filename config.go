package db

type Config struct {
	Host     string `yaml:"host" default:"localhost"`
	User     string `yaml:"user" default:"postgres"`
	Password string `yaml:"password" default:""`
	Database string `yaml:"database" default:"postgres"`
	Schema   string `yaml:"schema" default:"postgres"`
	Port     int    `yaml:"port" default:"5432"`
	SSLMode  bool   `yaml:"sslmode" default:"false"`
}
