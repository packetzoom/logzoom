package tcp

import (
	"fmt"
	"log"
	"net"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
	"github.com/packetzoom/logzoom/server"
	"gopkg.in/yaml.v2"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host string `yaml:"host"`
	SampleSize *int `yaml:"sample_size,omitempty"`
}

type TCPServer struct {
	name string
	fields map[string]string
	b    buffer.Sender
	term chan bool
	config *Config
}

func init() {
	output.Register("tcp", New)
}

func New() (output.Output) {
	return &TCPServer{term: make(chan bool, 1)}
}

// lumberConn handles an incoming connection from a lumberjack client
func (s *TCPServer) accept(c net.Conn) {
	defer func() {
		s.b.DelSubscriber(s.name)
		log.Printf("[%s - %s] closing tcp connection", s.name, c.RemoteAddr().String())
		c.Close()
	}()

	log.Printf("[%s - %s] accepting tcp connection", s.name, c.RemoteAddr().String())

	// Add the client as a subscriber
	r := make(chan *buffer.Event, recvBuffer)
	s.b.AddSubscriber(s.name, r)

	for {
		select {
		case ev := <-r:
			var allowed bool
			allowed = true
			for key, value :=  range s.fields {
				if ((*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string))) {
					allowed = false
					break
				}
                        }
                        if allowed && server.RandInt(0, 100) < *s.config.SampleSize {
				_, err := c.Write([]byte(fmt.Sprintf("%s %s\n", ev.Source, *ev.Text)))
				if err != nil {
					log.Printf("[%s - %s] error sending event to tcp connection: %v", s.name, c.RemoteAddr().String(), err)
					return
				}
			}
		}
	}

}

func (s *TCPServer) Init(name string, config yaml.MapSlice, b buffer.Sender, route route.Route) error {
	var tcpConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &tcpConfig); err != nil {
		return fmt.Errorf("Error parsing tcp config: %v", err)
	}

	s.name = name
	s.fields = route.Fields
	s.config = tcpConfig
	s.b = b
	return nil
}

func (s *TCPServer) Start() error {
	if (s.b == nil) {
		log.Printf("[%s] No Route is specified for this output", s.name)
		return nil
	}
	ln, err := net.Listen("tcp", s.config.Host)
	if err != nil {
		return fmt.Errorf("TCPServer: listener failed: %v", err)
	}

	if s.config.SampleSize == nil {
		i := 100
                s.config.SampleSize = &i
        }
        log.Printf("[%s] Setting Sample Size to %d", s.name, *s.config.SampleSize)

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
