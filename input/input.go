package input

import (
	"encoding/json"
	"fmt"

	"github.com/packetzoom/logslammer/parser"
)

type Input interface {
	Init(json.RawMessage, parser.Receiver) error
	Start() error
	Stop() error
}

var (
	inputs = make(map[string]Input)
)

func Register(name string, input Input) error {
	if _, ok := inputs[name]; ok {
		return fmt.Errorf("Input %s already exists", name)
	}
	inputs[name] = input
	return nil
}

func Load(name string) (Input, error) {
	input, ok := inputs[name]
	if !ok {
		return nil, fmt.Errorf("Input %s not found", name)
	}
	return input, nil
}
