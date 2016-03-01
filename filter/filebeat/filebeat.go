package filter

import (
	"fmt"
	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/filter"
	yaml_support "github.com/packetzoom/logslammer/yaml"
	"log"
)

type DocumentConfig struct {
	DocumentType string `yaml:"document_type"`
	Tag          string `yaml:"tag"`
	OutputMode   string `yaml:"output_mode"`
}

type FilebeatFilter struct {
	config   []DocumentConfig
	outputCh chan *buffer.Event
	term     chan bool
}

func init() {
	filter.Register("filebeat", &FilebeatFilter{
		term: make(chan bool, 1),
	})
}

func (f *FilebeatFilter) Init(yamlConfig yaml_support.RawMessage) error {
	filebeatConfig := &f.config

	log.Println("Starting Filebeat filter")

	if err := yamlConfig.Unmarshal(filebeatConfig); err != nil {
		return fmt.Errorf("Error parsing Filebeat config: %v", err)
	}

	return nil
}

func (f *FilebeatFilter) getConfig(eventType string) *DocumentConfig {
	for _, config := range f.config {
		log.Printf("checking document type %s\n", config.DocumentType)
		if config.DocumentType == eventType {
			return &config
		}
	}

	return nil
}

func (f *FilebeatFilter) handleData(ev *buffer.Event, out chan *buffer.Event) {
	fields := *ev.Fields
	outputFields := make(map[string]interface{})
	eventType := fields["type"].(string)

	config := f.getConfig(eventType)

	if config == nil {
		log.Printf("Ignoring event type: %s", eventType)
		return
	}

	log.Println("Got output mode:", config.OutputMode)

	ev.Tag = config.Tag

	switch config.OutputMode {
	case "standard":
		ev.OutputFields = &fields

		if beatData, ok := fields["beat"].(map[string]string); ok {
			(*ev.OutputFields)["host"] = beatData["hostname"]

			if ts, ok := fields["@timestamp"].(string); ok {
				(*ev.OutputFields)["@timestamp"] = ts
			}
		}
	case "json_passthrough":
		t := fields["message"].(string)
		ev.Text = &t
	}

	log.Printf("Output fields:", outputFields)
	out <- ev
}

func (f *FilebeatFilter) Process(in chan *buffer.Event) chan *buffer.Event {
	out := make(chan *buffer.Event)

	go func() {
		for {
			select {
			case i := <-in:
				f.handleData(i, out)
			}
		}
	}()

	f.outputCh = out
	return f.outputCh
}
