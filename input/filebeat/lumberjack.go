package filebeat

import (
	"crypto/tls"
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"net"

	"github.com/packetzoom/logzoom/input"
)

type Config struct {
	Host   string `yaml:"host"`
	SSLCrt string `yaml:"ssl_crt"`
	SSLKey string `yaml:"ssl_key"`
}

type LJServer struct {
	Config *Config
	r      input.Receiver
	term   chan bool
}

// lumberConn handles an incoming connection from a lumberjack client
func lumberConn(c net.Conn, r input.Receiver) {
	defer c.Close()
	log.Printf("[%s] accepting lumberjack connection", c.RemoteAddr().String())
	NewParser(c, r).Parse()
	log.Printf("[%s] closing lumberjack connection", c.RemoteAddr().String())
}

func (lj *LJServer) Init(config yaml.MapSlice, r input.Receiver) error {
	var ljConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &ljConfig); err != nil {
		return fmt.Errorf("Error parsing lumberjack config: %v", err)
	}

	lj.Config = ljConfig
	lj.r = r

	return nil
}

func (lj *LJServer) Start() error {
	cert, err := tls.LoadX509KeyPair(lj.Config.SSLCrt, lj.Config.SSLKey)
	if err != nil {
		return fmt.Errorf("Error loading keys: %v", err)
	}

	conn, err := net.Listen("tcp", lj.Config.Host)
	if err != nil {
		return fmt.Errorf("Listener failed: %v", err)
	}

	config := tls.Config{Certificates: []tls.Certificate{cert}}

	ln := tls.NewListener(conn, &config)

	for {
		select {
		case <-lj.term:
			log.Println("Lumberjack server received term signal")
			return nil
		default:
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			go lumberConn(conn, lj.r)
		}
	}

	return nil
}

func (lj *LJServer) Stop() error {
	lj.term <- true
	return nil
}
