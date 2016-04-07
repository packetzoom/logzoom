# LogZoom - A Lumberjack => Logstash indexer in Go

LogZoom is a lightweight, Lumberjack-compliant log indexer based off the fine
work of Hailo's [Logslam](https://github.com/hailocab/logslam). It accepts
the Lumberjack v2 protocol, which is currently supported by [Elastic's Filebeat]
(https://github.com/elastic/beats).

It was written with the intention of being a smaller, efficient, and more reliable
replacement for logstash and logslam.

## Supported IO

### Inputs

- Filebeat (Lumberjack V2 Protocol)
- Redis Message Queue

### Outputs

- Redis Message Queue
- TCP Streaming
- WebSocket Streaming
- Elasticsearch
- S3

## Getting Started

### 1. Create config

Create a YAML config file specifying the desired input and outputs. An example
config can be found in examples/example.config.yml:

```yaml
inputs:
  filebeat:
    host: 0.0.0.0:7200
    ssl_crt: /etc/filebeat/filebeat.crt
    ssl_key: /etc/filebeat/filebeat.key
outputs:
  tcp:
    host: :7201
  websocket:
    host: :7202
  elasticsearch:
    hosts:
      - http://localhost:9200
``````

### 2. Run the server

```
$ go get
$ $GOPATH/bin/logslammer -config=examples/example.config.yml
2015/01/20 20:59:03 Starting server
2015/01/20 20:59:03 Starting buffer
2015/01/20 20:59:03 Starting input filebeat
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

### Elasticsearch support

Note that currently only Elasticsearch 1.x is supported. If you need 2.x
support, I think it is just a matter of updating [Olliver Eilhard's 3.x
client](https://github.com/olivere/elastic#releases).
