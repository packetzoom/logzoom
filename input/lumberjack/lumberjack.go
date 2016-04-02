package lumberjack

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"

	"github.com/packetzoom/logslammer/input"
	"github.com/packetzoom/logslammer/parser"
	yaml_support "github.com/packetzoom/logslammer/yaml"
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

func init() {
	input.Register("lumberjack", &LJServer{
		term: make(chan bool, 1),
	})
}

// lumberConn handles an incoming connection from a lumberjack client
func lumberConn(c net.Conn, r input.Receiver) {
	defer c.Close()
	log.Printf("[%s] accepting lumberjack connection", c.RemoteAddr().String())
	parser.New(c, r).Parse()
	log.Printf("[%s] closing lumberjack connection", c.RemoteAddr().String())
}

func (lj *LJServer) Init(yamlConfig yaml_support.RawMessage, r input.Receiver) error {
	var ljConfig *Config

	if err := yamlConfig.Unmarshal(&ljConfig); err != nil {
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
