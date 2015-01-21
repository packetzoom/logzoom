package main

import (
	"flag"
	"fmt"
	"os"

	_ "github.com/hailocab/logslam/input/lumberjack"
	_ "github.com/hailocab/logslam/output/elasticsearch"
	_ "github.com/hailocab/logslam/output/tcp"
	_ "github.com/hailocab/logslam/output/websocket"
	"github.com/hailocab/logslam/server"
)

var (
	config string
)

func init() {
	flag.StringVar(&config, "config", "", "Path to the config file")
	flag.Parse()

	if len(config) == 0 {
		fmt.Fprintln(os.Stderr, "Require a config file")
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func main() {
	srv, err := server.New(config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	srv.Start()
}
