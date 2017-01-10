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
	SampleSize *int `yaml:"sample_size,omitempty"`
}

type LJServer struct {
	name   string
	Config *Config
	r      input.Receiver
	term   chan bool
}

func New() input.Input {
        return &LJServer{term: make(chan bool, 1)}
}

// lumberConn handles an incoming connection from a lumberjack client
func lumberConn(c net.Conn, r input.Receiver, sampleSize int) {
	defer c.Close()
	log.Printf("[%s] accepting lumberjack connection", c.RemoteAddr().String())
	NewParser(c, r, sampleSize).Parse()
	log.Printf("[%s] closing lumberjack connection", c.RemoteAddr().String())
}

func (lj *LJServer) Init(name string, config yaml.MapSlice, r input.Receiver) error {
	var ljConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &ljConfig); err != nil {
		return fmt.Errorf("Error parsing lumberjack config: %v", err)
	}

	lj.name = name
	lj.Config = ljConfig
	lj.r = r

	return nil
}

func (lj *LJServer) Start() error {
	cert, err := tls.LoadX509KeyPair(lj.Config.SSLCrt, lj.Config.SSLKey)
	if err != nil {
		return fmt.Errorf("Error loading keys: %v", err)
	}

	if lj.Config.SampleSize == nil {
		i := 100
		lj.Config.SampleSize = &i
	}
	log.Printf("[%s] Setting Sample Size to %d%%", lj.name, *lj.Config.SampleSize)

	conn, err := net.Listen("tcp", lj.Config.Host)
	if err != nil {
		return fmt.Errorf("Listener failed: %v", err)
	}

	config := tls.Config{Certificates: []tls.Certificate{cert}}

	ln := tls.NewListener(conn, &config)

	log.Printf("[%s] Started Lumberjack Instance", lj.name)
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
			go lumberConn(conn, lj.r, *lj.Config.SampleSize)
		}
	}

	return nil
}

func (lj *LJServer) Stop() error {
	lj.term <- true
	return nil
}
