package input

import (
	"fmt"
	"gopkg.in/yaml.v2"

	"github.com/packetzoom/logzoom/buffer"
)

type Receiver interface {
	Send(*buffer.Event)
}

type Input interface {
	Init(string, yaml.MapSlice, Receiver) error
	Start() error
	Stop() error
}

var (
	inputs = make(map[string]func()Input)
)

func Register(name string, constructor func()Input) error {
	if _, ok := inputs[name]; ok {
		return fmt.Errorf("Input %s already exists", name)
	}
	inputs[name] = constructor
	return nil
}

func Load(name string) (Input, error) {
	constructor, ok := inputs[name]
	if !ok {
		return nil, fmt.Errorf("Constructor %s not found", name)
	}
	return constructor(), nil
}
