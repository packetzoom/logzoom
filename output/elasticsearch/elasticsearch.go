package elasticsearch

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"
	"gopkg.in/olivere/elastic.v2"
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
	bulkService *elastic.BulkService
	indexPrefix string
	indexType   string
}

type Config struct {
	Hosts       []string `json:"hosts"`
	IndexPrefix string   `json:"index"`
	IndexType   string   `json:"indexType"`
}

type ESServer struct {
	config Config
	host   string
	hosts  []string
	b      buffer.Sender
	term   chan bool
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

func indexDoc(ev *buffer.Event) *map[string]interface{} {
	return &*ev.Fields
}

func (i *Indexer) flush() {
	log.Printf("Flushing %d event(s) to elasticsearch", i.bulkService.NumberOfActions())
	i.bulkService.Do()
}

func (i *Indexer) index(ev *buffer.Event) {
	log.Printf("Received event here")
	doc := indexDoc(ev)
	idx := indexName(i.indexPrefix)
	typ := i.indexType

	request := elastic.NewBulkIndexRequest().Index(idx).Type(typ).Doc(doc)
	i.bulkService.Add(request)
	numEvents := i.bulkService.NumberOfActions()

	if numEvents < esSendBuffer {
		return
	}

	i.flush()
}

func (e *ESServer) Init(config json.RawMessage, b buffer.Sender) error {
	var esConfig *Config
	if err := json.Unmarshal(config, &esConfig); err != nil {
		return fmt.Errorf("Error parsing elasticsearch config: %v", err)
	}

	e.config = *esConfig
	e.hosts = esConfig.Hosts
	e.b = b

	return nil
}

func (es *ESServer) Start() error {
	client, err := elastic.NewClient(elastic.SetURL(es.hosts...))

	if err != nil {
		log.Printf("Error starting Elasticsearch: %s", err)
		return err
	}

	service := elastic.NewBulkService(client)

	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, esRecvBuffer)
	es.b.AddSubscriber(es.host, receiveChan)
	defer es.b.DelSubscriber(es.host)

	// Create indexer
	idx := &Indexer{service, es.config.IndexPrefix, es.config.IndexType}

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
