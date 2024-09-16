package config

var (
	Host string = "0.0.0.0"
	Port int    = 6379

	RedisVer = "6.0.16"
	RdbVer   = "0011"

	DefaultMessageSize = 1024
	MaximumClients     = 100
	MaximumReplicas    = 100

	RdbDir      string
	RdbFileName string

	//Persistence
	NumKeyChanges = 1
	Interval      = 30 //seconds
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
