# Logslammer- A Lumberjack => Logstash indexer in Go

Logslammer is a lightweight, Lumberjack-compliant log indexer based off the fine
work of Hailo's [Logslam](https://github.com/hailocab/logslam). It accepts
the Lumberjack v2 protocol, which is currently supported by [Elastic's Filebeat]
(https://github.com/elastic/beats).

It was written with the intention of being a smaller, efficient, and more reliable
replacement for logstash and logslam.

## Supported IO

### Inputs

- Lumberjack V2 Protocol
- Redis Message Queue

### Outputs

- Redis Message Queue
- TCP Streaming
- WebSocket Streaming
- Elasticsearch
- S3

## Getting Started

### 1. Create config

Create a config file specifying location of ssl crt/key, bind addresses for inputs/outputs and elasticsearch hosts.

An example config can be found in example.config.json:
```
{
	"inputs": {
		"lumberjack": {
			"host": ":7200",
			"ssl_key": "lumberjack.key",
			"ssl_crt": "lumberjack.crt"
		}
	},
	"outputs": {
		"tcp": {
			"host": ":7201"
		},
		"websocket": {
			"host": ":7202"
		},
		"elasticsearch": {
			"hosts": [
				"localhost:9200"
			]
		}
	}
}
```

### 2. Run the server

```
$ go get
$ $GOPATH/bin/logslam -config=example.config.json
2015/01/20 20:59:03 Starting server
2015/01/20 20:59:03 Starting buffer
2015/01/20 20:59:03 Starting input lumberjack
2015/01/20 20:59:03 Starting output tcp
2015/01/20 20:59:03 Starting output websocket
2015/01/20 20:59:03 Starting output elasticsearch
```

### Streaming logs via TCP

```
nc localhost 7201
```

### Streaming logs via WebSocket

```
Connect to http://localhost:7202 in a browser.
A list of known sources will be displayed.
```
