# Logslam - A lumberjack => logstash indexer in Go

Logslam is a lightweight lumberjack compliant log indexer. It accepts the lumberjack v1 protocol and indexes logs in elasticsearch. It was written with the intention of being a small and efficient replacement for logstash on AWS EC2. It does not attempt to replicate all of logstash's features, the goal was to simply replace it for Hailo's specific use case in the ELK stack.

## Supported IO

### Inputs

- Lumberjack V1 Protocol

### Outputs

- TCP Streaming
- WebSocket Streaming
- Elasticsearch

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
