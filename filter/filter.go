package filter

import (
	"fmt"

	"github.com/packetzoom/logslammer/buffer"
	yaml_support "github.com/packetzoom/logslammer/yaml"
)

type Filter interface {
	Init(yaml_support.RawMessage) error
	Process(in chan *buffer.Event) chan *buffer.Event
}

var (
	filters = make(map[string]Filter)
)

func Register(name string, filter Filter) error {
	if _, ok := filters[name]; ok {
		return fmt.Errorf("Filter %s already exists", name)
	}
	filters[name] = filter
	return nil
}

func Load(name string) (Filter, error) {
	filter, ok := filters[name]
	if !ok {
		return nil, fmt.Errorf("Filter %s not found", name)
	}
	return filter, nil
}
