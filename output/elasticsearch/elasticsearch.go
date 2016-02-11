package elasticsearch

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"
	"github.com/paulbellamy/ratecounter"
	"gopkg.in/olivere/elastic.v2"
	"gopkg.in/yaml.v2"
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
	Hosts           []string `yaml:"hosts"`
	IndexPrefix     string   `yaml:"index"`
	IndexType       string   `yaml:"index_type"`
	Timeout         int      `yaml:"timeout"`
	GzipEnabled     bool     `yaml:"gzip_enabled"`
	InfoLogEnabled  bool     `yaml:"info_log_enabled"`
	ErrorLogEnabled bool     `yaml:"error_log_enabled"`
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

func (e *ESServer) ValidateConfig(config *Config) error {
	if len(config.Hosts) == 0 {
		return errors.New("Missing hosts")
	}

	if len(config.IndexPrefix) == 0 {
		return errors.New("Missing index prefix (e.g. logstash)")
	}

	if len(config.IndexType) == 0 {
		return errors.New("Missing index type (e.g. logstash)")
	}

	return nil
}

func (e *ESServer) Init(config yaml.MapSlice, b buffer.Sender) error {
	var esConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &esConfig); err != nil {
		return fmt.Errorf("Error parsing elasticsearch config: %v", err)
	}

	e.config = *esConfig
	e.hosts = esConfig.Hosts
	e.b = b

	return nil
}

func readInputChannel(idx *Indexer, receiveChan chan *buffer.Event) {
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

func (es *ESServer) Start() error {
	var client *elastic.Client
	var err error

	for {
		httpClient := http.DefaultClient
		timeout := 60 * time.Second

		if es.config.Timeout > 0 {
			timeout = time.Duration(es.config.Timeout) * time.Second
		}

		log.Println("Setting HTTP timeout to", timeout)
		log.Println("Setting GZIP enabled:", es.config.GzipEnabled)

		httpClient.Timeout = timeout

		var infoLogger, errorLogger *log.Logger

		if es.config.InfoLogEnabled {
			infoLogger = log.New(os.Stdout, "", log.LstdFlags)
		}

		if es.config.ErrorLogEnabled {
			errorLogger = log.New(os.Stderr, "", log.LstdFlags)
		}

		client, err = elastic.NewClient(elastic.SetURL(es.hosts...),
			elastic.SetHttpClient(httpClient),
			elastic.SetGzip(es.config.GzipEnabled),
			elastic.SetInfoLog(infoLogger),
			elastic.SetErrorLog(errorLogger))

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

		if len(tick.C) > 0 || len(es.term) > 0 {
			select {
			case <-tick.C:
				idx.flush()
			case <-es.term:
				tick.Stop()
				log.Println("Elasticsearch received term signal")
				break
			}
		}
	}

	return nil
}

func (es *ESServer) Stop() error {
	es.term <- true
	return nil
}
