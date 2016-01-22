package elasticsearch

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"
	"github.com/paulbellamy/ratecounter"
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
	bulkService       *elastic.BulkService
	indexPrefix       string
	indexType         string
	RateCounter       *ratecounter.RateCounter
	lastDisplayUpdate time.Time
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

func (i *Indexer) flush() error {
	numEvents := i.bulkService.NumberOfActions()

	if numEvents > 0 {
		if time.Now().Sub(i.lastDisplayUpdate) >= time.Duration(1*time.Second) {
			log.Printf("Flushing %d event(s) to Elasticsearch, current rate: %d/s", numEvents, i.RateCounter.Rate())
			i.lastDisplayUpdate = time.Now()
		}

		_, err := i.bulkService.Do()

		if err != nil {
			log.Printf("Unable to flush events: %s", err)
		}

		return err
	}

	return nil
}

func (i *Indexer) index(ev *buffer.Event) error {
	doc := *ev.Text
	idx := indexName(i.indexPrefix)
	typ := i.indexType

	request := elastic.NewBulkIndexRequest().Index(idx).Type(typ).Doc(doc)
	i.bulkService.Add(request)
	i.RateCounter.Incr(1)

	numEvents := i.bulkService.NumberOfActions()

	if numEvents < esSendBuffer {
		return nil
	}

	return i.flush()
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

func readInputChannel(idx *Indexer, receiveChan chan *buffer.Event) {
	for {
		// Drain the channel only if we have room
		if idx.bulkService.NumberOfActions() < esSendBuffer {
			select {
			case ev := <-receiveChan:
				idx.index(ev)
			}
		} else {
			log.Printf("Internal Elasticsearch buffer is full, waiting")
			time.Sleep(1 * time.Second)
		}
	}
}

func (es *ESServer) Start() error {
	var client *elastic.Client
	var err error

	for {
		client, err = elastic.NewClient(elastic.SetURL(es.hosts...))
		if err != nil {
			log.Printf("Error starting Elasticsearch: %s, will retry", err)
			time.Sleep(2 * time.Second)
			continue
		}

		break
	}

	log.Printf("Connected to Elasticsearch")

	service := elastic.NewBulkService(client)

	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, esRecvBuffer)
	es.b.AddSubscriber(es.host, receiveChan)
	defer es.b.DelSubscriber(es.host)

	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	// Create indexer
	idx := &Indexer{service, es.config.IndexPrefix, es.config.IndexType, rateCounter, time.Now()}

	// Loop events and publish to elasticsearch
	tick := time.NewTicker(time.Duration(esFlushInterval) * time.Second)

	for {
		readInputChannel(idx, receiveChan)

		select {
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
