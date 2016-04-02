package yaml

// go-yaml doesn't have a great way to defer unmarshaling of YAML data.
// This implementation is used to help support this.
// See https://github.com/go-yaml/yaml/issues/13
type RawMessage struct {
	unmarshal func(interface{}) error
}

func (msg *RawMessage) UnmarshalYAML(unmarshal func(interface{}) error) error {
	msg.unmarshal = unmarshal
	return nil
}

func (msg *RawMessage) Unmarshal(v interface{}) error {
	return msg.unmarshal(v)
}
