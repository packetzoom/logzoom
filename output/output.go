package output

import (
	"fmt"
	"gopkg.in/yaml.v2"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/route"
)

type Output interface {
	Init(string, yaml.MapSlice, buffer.Sender, route.Route) error
	Start() error
	Stop() error
}

var (
	outputs = make(map[string]func()Output)
)

func Register(name string, constructor func()Output) error {
	if _, ok := outputs[name]; ok {
		return fmt.Errorf("Output %s already exists", name)
	}
	outputs[name] = constructor
	return nil
}

func Load(name string) (Output, error) {
	constructor, ok := outputs[name]
	if !ok {
		return nil, fmt.Errorf("Output %s not found", name)
	}
	return constructor(), nil
}
