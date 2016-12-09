package stdin

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/input"
	"gopkg.in/yaml.v2"
)

func init() {
	input.Register("stdin", New)
}

func New() input.Input {
	return &stdInput{}
}

type Config struct {
	JsonDecode bool `yaml:"json_decode"`
}

type stdInput struct {
	receiver input.Receiver
	config   Config
}

func (t *stdInput) Init(name string, config yaml.MapSlice, r input.Receiver) error {
	if r == nil {
		return fmt.Errorf("no receiver")
	}

	var pluginConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &pluginConfig); err != nil {
		return fmt.Errorf("Error parsing Stdin plugin config: %v", err)
	}

	t.receiver = r
	t.config = *pluginConfig
	return nil
}

func (t *stdInput) Start() error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		ev, err := t.parseEvent(scanner.Text())

		if err != nil {
			continue
		}

		t.receiver.Send(ev)
	}
	return nil
}

func (t *stdInput) parseEvent(payload string) (*buffer.Event, error) {
	ev := &buffer.Event{Text: &payload}

	if t.config.JsonDecode {
		decoder := json.NewDecoder(strings.NewReader(payload))
		decoder.UseNumber()

		err := decoder.Decode(&ev.Fields)

		if err != nil {
			return nil, err
		}
	}

	return ev, nil
}

func (t *stdInput) Stop() error {
	return nil
}
