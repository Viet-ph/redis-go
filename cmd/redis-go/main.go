package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Viet-ph/redis-go/config"
	"github.com/Viet-ph/redis-go/core"
	"github.com/Viet-ph/redis-go/server"
)

func setupFlags() {
	flag.StringVar(&config.Host, "host", "0.0.0.0", "host for the redis server")
	flag.StringVar(&core.Master, "replicaof", "", "master instance at <MASTER_HOST> <MASTER_PORT>")
	flag.IntVar(&config.Port, "port", 6379, "port for the redis server")
	flag.Parse()
}

func main() {
	setupFlags()
	flag.PrintDefaults()

	fmt.Println("Setting up master/slave ...")
	connToMaster, err := core.SetupMasterSlave()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("Master/slave setup done.")

	if connToMaster == nil {
		fmt.Println("Setting up master server ...")
		srv, err := server.NewAsyncServer()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		srv.Start()
	} else {
		fmt.Println("Setting up slave server ...")
		srv := server.NewRepServer()
		srv.Start(connToMaster)
	}

}
