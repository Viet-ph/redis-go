package config

var (
	Host string = "0.0.0.0"
	Port int    = 6379

	DefaultMessageSize = 1024
	MaximumClients     = 100
	MaximumReplicas    = 100

	RdbDir      string
	RdbFileName string
)

func GetConfigValue(cfgName string) (any, bool) {
	switch cfgName {
	case "dir":
		return RdbDir, true
	case "dbfilename":
		return RdbFileName, true
	default:
		return nil, false
	}
}
