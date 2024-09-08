package config

var (
	Host string = "0.0.0.0"
	Port int    = 6379

	DefaultMessageSize = 1024
	MaximumClients     = 100
	MaximumReplicas    = 100
)
