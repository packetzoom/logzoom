package server

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/input"
	"github.com/packetzoom/logslammer/output"
)

type Server struct {
	Config *Config
	Buffer *buffer.Buffer

	mtx     sync.Mutex
	inputs  map[string]input.Input
	outputs map[string]output.Output
}

func signalCatcher() chan os.Signal {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	return c
}

func New(configFile string) (*Server, error) {
	config, err := LoadConfig(configFile)
	if err != nil {
		return nil, err
	}

	return &Server{
		Config:  config,
		Buffer:  buffer.New(),
		inputs:  make(map[string]input.Input),
		outputs: make(map[string]output.Output),
	}, nil
}

func (s *Server) Start() {
	log.Println("Starting server")

	// Start buffer
	log.Println("Starting buffer")
	go s.Buffer.Start()

	s.mtx.Lock()

	// Start inputs
	for name, config := range s.Config.Inputs {
		in, err := input.Load(name)
		if err != nil {
			log.Println(err.Error)
			continue
		}

		if err := in.Init(config, s.Buffer); err != nil {
			log.Fatalf("Failed to init %s input: %v", name, err)
		}

		go func(name string, in input.Input) {
			log.Printf("Starting input %s", name)
			if err := in.Start(); err != nil {
				log.Fatalf("Error starting input %s: %v", name, err)
			}
		}(name, in)

		s.inputs[name] = in
	}

	// Start outputs
	for name, config := range s.Config.Outputs {
		out, err := output.Load(name)
		if err != nil {
			log.Println(err.Error)
			continue
		}

		if err := out.Init(config, s.Buffer); err != nil {
			log.Fatalf("Failed to init %s output: %v", name, err)
			continue
		}

		go func(name string, out output.Output) {
			log.Printf("Starting output %s", name)
			if err := out.Start(); err != nil {
				log.Fatalf("Error starting output %s: %v", name, err)
			}
		}(name, out)

		s.outputs[name] = out
	}

	s.mtx.Unlock()

	// Wait for kill signal
	<-signalCatcher()
	log.Printf("Received quit signal")

	// Stop Server
	s.Stop()
}

func (s *Server) Stop() {
	log.Println("Stopping server")

	s.mtx.Lock()

	// stop inputs
	for name, in := range s.inputs {
		log.Printf("Stopping input %s", name)
		if err := in.Stop(); err != nil {
			log.Printf("Error stopping %s input: %v", name, err)
		}
	}

	// stop ouputs
	for name, out := range s.outputs {
		log.Printf("Stopping output %s", name)
		if err := out.Stop(); err != nil {
			log.Printf("Error stopping %s output: %v", name, err)
		}
	}

	s.mtx.Unlock()

	log.Println("Stopping buffer")
	s.Buffer.Stop()
}
