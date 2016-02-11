package tcp

import (
	"fmt"
	"log"
	"net"

	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"
	"gopkg.in/yaml.v2"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host string `yaml:"host"`
}

type TCPServer struct {
	host string
	b    buffer.Sender
	term chan bool
}

func init() {
	output.Register("tcp", &TCPServer{
		term: make(chan bool, 1),
	})
}

// lumberConn handles an incoming connection from a lumberjack client
func (s *TCPServer) accept(c net.Conn) {
	defer func() {
		s.b.DelSubscriber(c.RemoteAddr().String())
		log.Printf("[%s] closing tcp connection", c.RemoteAddr().String())
		c.Close()
	}()

	log.Printf("[%s] accepting tcp connection", c.RemoteAddr().String())

	// Add the client as a subscriber
	r := make(chan *buffer.Event, recvBuffer)
	s.b.AddSubscriber(c.RemoteAddr().String(), r)

	for {
		select {
		case ev := <-r:
			_, err := c.Write([]byte(fmt.Sprintf("%s %s\n", ev.Source, *ev.Text)))
			if err != nil {
				log.Printf("[%s] error sending event to tcp connection: %v", c.RemoteAddr().String(), err)
				return
			}
		}
	}

}

func (s *TCPServer) Init(config yaml.MapSlice, b buffer.Sender) error {
	var tcpConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &tcpConfig); err != nil {
		return fmt.Errorf("Error parsing tcp config: %v", err)
	}

	s.host = tcpConfig.Host
	s.b = b
	return nil
}

func (s *TCPServer) Start() error {
	ln, err := net.Listen("tcp", s.host)
	if err != nil {
		return fmt.Errorf("TCPServer: listener failed: %v", err)
	}

	for {
		select {
		case <-s.term:
			log.Println("TCPServer received term signal")
			return nil
		default:
			conn, err := ln.Accept()
			if err != nil {
				log.Println("Error accepting tcp connection: %v", err)
				continue
			}
			go s.accept(conn)
		}
	}

	return nil
}

func (s *TCPServer) Stop() error {
	s.term <- true
	return nil
}
