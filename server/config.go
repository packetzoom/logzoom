package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	Inputs  map[string]json.RawMessage `json:"inputs"`
	Outputs map[string]json.RawMessage `json:"outputs"`
}

func LoadConfig(file string) (*Config, error) {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("Could not read config file %s: %v", file, err)
	}

	var conf *Config
	err = json.Unmarshal(b, &conf)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse config %s: %v", file, err)
	}

	return conf, nil
}
