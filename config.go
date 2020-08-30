package rithdb

type ConnectionConfig struct {
	Driver   string `yaml:"driver"`
	Host     string `yaml:"host"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
}

type Config struct {
	Default     string                      `yaml:"default"`
	Connections map[string]ConnectionConfig `yaml:"connections"`
}
