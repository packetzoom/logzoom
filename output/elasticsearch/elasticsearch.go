package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/hailocab/elastigo/api"
	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"
)

const (
	defaultHost        = "127.0.0.1"
	defaultIndexPrefix = "logstash"
	esFlushInterval    = 5
	esMaxConns         = 20
	esRecvBuffer       = 100
	esSendBuffer       = 100
)

type Indexer struct {
	events int
	buffer *bytes.Buffer
}

type Config struct {
	Hosts []string `json:"hosts"`
}

type ESServer struct {
	host  string
	hosts []string
	b     buffer.Sender
	term  chan bool
}

func init() {
	output.Register("elasticsearch", &ESServer{
		host: fmt.Sprintf("%s:%d", defaultHost, time.Now().Unix()),
		term: make(chan bool, 1),
	})
}

func indexName(idx string) string {
	if len(idx) == 0 {
		idx = defaultIndexPrefix
	}

	return fmt.Sprintf("%s-%s", idx, time.Now().Format("2006.01.02"))
}

func bulkSend(buf *bytes.Buffer) error {
	_, err := api.DoCommand("POST", "/_bulk", buf)
	if err != nil {
		return err
	}

	return nil
}

func indexDoc(ev *buffer.Event) *map[string]interface{} {
	f := *ev.Fields
	host := f["host"]
	file := f["file"]
	timestamp := f["timestamp"]
	message := strconv.Quote(*ev.Text)

	delete(f, "timestamp")
	delete(f, "line")
	delete(f, "host")
	delete(f, "file")

	return &map[string]interface{}{
		"@type":        f["type"],
		"@message":     &message,
		"@source_path": file,
		"@source_host": host,
		"@timestamp":   timestamp,
		"@fields":      &f,
		"@source":      ev.Source,
	}
}

func (i *Indexer) writeBulk(index string, _type string, data interface{}) error {
	w := `{"index":{"_index":"%s","_type":"%s"}}`

	i.buffer.WriteString(fmt.Sprintf(w, index, _type))
	i.buffer.WriteByte('\n')

	switch v := data.(type) {
	case *bytes.Buffer:
		io.Copy(i.buffer, v)
	case []byte:
		i.buffer.Write(v)
	case string:
		i.buffer.WriteString(v)
	default:
		body, err := json.Marshal(data)
		if err != nil {
			log.Printf("Error writing bulk data: %v", err)
			return err
		}
		i.buffer.Write(body)
	}
	i.buffer.WriteByte('\n')
	return nil
}

func (i *Indexer) flush() {
	if i.events == 0 {
		return
	}

	log.Printf("Flushing %d event(s) to elasticsearch", i.events)
	for j := 0; j < 3; j++ {
		if err := bulkSend(i.buffer); err != nil {
			log.Printf("Failed to index event (will retry): %v", err)
			time.Sleep(time.Duration(50) * time.Millisecond)
			continue
		}
		break
	}

	i.buffer.Reset()
	i.events = 0
}

func (i *Indexer) index(ev *buffer.Event) {
	doc := indexDoc(ev)
	idx := indexName("")
	typ := (*ev.Fields)["type"].(string)

	i.events++
	i.writeBulk(idx, typ, doc)

	if i.events < esSendBuffer {
		return
	}

	log.Printf("Flushing %d event(s) to elasticsearch", i.events)
	for j := 0; j < 3; j++ {
		if err := bulkSend(i.buffer); err != nil {
			log.Printf("Failed to index event (will retry): %v", err)
			time.Sleep(time.Duration(50) * time.Millisecond)
			continue
		}
		break
	}

	i.buffer.Reset()
	i.events = 0
}

func (e *ESServer) Init(config json.RawMessage, b buffer.Sender) error {
	var esConfig *Config
	if err := json.Unmarshal(config, &esConfig); err != nil {
		return fmt.Errorf("Error parsing elasticsearch config: %v", err)
	}

	e.hosts = esConfig.Hosts
	e.b = b

	return nil
}

func (es *ESServer) Start() error {
	api.SetHosts(es.hosts)

	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, esRecvBuffer)
	es.b.AddSubscriber(es.host, receiveChan)
	defer es.b.DelSubscriber(es.host)

	// Create indexer
	idx := &Indexer{0, bytes.NewBuffer(nil)}

	// Loop events and publish to elasticsearch
	tick := time.NewTicker(time.Duration(esFlushInterval) * time.Second)

	for {
		select {
		case ev := <-receiveChan:
			idx.index(ev)
		case <-tick.C:
			idx.flush()
		case <-es.term:
			tick.Stop()
			log.Println("Elasticsearch received term signal")
			break
		}
	}

	return nil
}

func (es *ESServer) Stop() error {
	es.term <- true
	return nil
}
