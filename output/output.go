package output

import (
	"fmt"
	"gopkg.in/yaml.v2"

	"github.com/packetzoom/logzoom/buffer"
)

type Output interface {
	Init(yaml.MapSlice, buffer.Sender) error
	Start() error
	Stop() error
}

var (
	outputs = make(map[string]Output)
)

func Register(name string, output Output) error {
	if _, ok := outputs[name]; ok {
		return fmt.Errorf("Output %s already exists", name)
	}
	outputs[name] = output
	return nil
}

func Load(name string) (Output, error) {
	output, ok := outputs[name]
	if !ok {
		return nil, fmt.Errorf("Output %s not found", name)
	}
	return output, nil
}
