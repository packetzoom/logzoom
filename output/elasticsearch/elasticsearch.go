package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
	"golang.org/x/net/context"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
	"github.com/packetzoom/logzoom/server"
	"github.com/paulbellamy/ratecounter"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/yaml.v2"
)

const (
	defaultHost        = "127.0.0.1"
	defaultIndexPrefix = "logstash"
	esFlushInterval    = 10
	esRecvBuffer       = 10000
	esSendBuffer       = 10000
	esWorker           = 20
	esBulkLimit        = 10000
)

type Indexer struct {
	bulkProcessor     *elastic.BulkProcessor
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
	SampleSize      *int      `yaml:"sample_size,omitempty"`
}

type ESServer struct {
	name   string
	fields map[string]string
	config Config
	host   string
	hosts  []string
	b      buffer.Sender
	term   chan bool
	idx    *Indexer
}

func init() {
	output.Register("elasticsearch", New)
}

func New() (output.Output) {
	return &ESServer{
		host: fmt.Sprintf("%s:%d", defaultHost, time.Now().Unix()),
		term: make(chan bool, 1),
	}
}

// Dummy discard, satisfies io.Writer without importing io or os.
type DevNull struct{}

func (DevNull) Write(p []byte) (int, error) {
	return len(p), nil
}

func indexName(idx string) string {
	if len(idx) == 0 {
		idx = defaultIndexPrefix
	}

	return fmt.Sprintf("%s-%s", idx, time.Now().Format("2006.01.02"))
}

func (i *Indexer) index(ev *buffer.Event) error {
	doc := *ev.Text
	idx := indexName(i.indexPrefix)
	typ := i.indexType

	request := elastic.NewBulkIndexRequest().Index(idx).Type(typ).Doc(doc)
	i.bulkProcessor.Add(request)
	i.RateCounter.Incr(1)

	return nil
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

	if e.config.SampleSize == nil {
		i := 100
		e.config.SampleSize = &i
	}
	log.Printf("[%s] Setting Sample Size to %d", e.name, *e.config.SampleSize)

	return nil
}

func (e *ESServer) Init(name string, config yaml.MapSlice, b buffer.Sender, route route.Route) error {
	var esConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &esConfig); err != nil {
		return fmt.Errorf("Error parsing elasticsearch config: %v", err)
	}

	e.name = name
	e.fields = route.Fields
	e.config = *esConfig
	e.hosts = esConfig.Hosts
	e.b = b

	if err := e.ValidateConfig(esConfig); err != nil {
		return fmt.Errorf("Error in config: %v", err)
	}

	return nil
}

func readInputChannel(sampleSize int, idx *Indexer, receiveChan chan *buffer.Event) {
	select {
		case ev := <-receiveChan:
			if (server.RandInt(0, 100) < sampleSize) {
				idx.index(ev)
			}
	}
}

func (es *ESServer) insertIndexTemplate(client *elastic.Client) error {
	var template map[string]interface{}
	err := json.Unmarshal([]byte(IndexTemplate), &template)

	if err != nil {
		return err
	}

	template["template"] = es.config.IndexPrefix + "-*"

	inserter := elastic.NewIndicesPutTemplateService(client)
	inserter.Name(es.config.IndexPrefix)
	inserter.Create(true)
	inserter.BodyJson(template)

	response, err := inserter.Do(context.Background())

	if response != nil {
		log.Println("Inserted template response:", response.Acknowledged)
	}

	return err
}

func (es *ESServer) afterCommit(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if (es.idx.RateCounter.Rate() > 0) {
		log.Printf("Flushed events to Elasticsearch, current rate: %d/s", es.idx.RateCounter.Rate())
	}
}

func (es *ESServer) Start() error {
	if (es.b == nil) {
		log.Printf("[%s] No Route is specified for this output", es.name)
		return nil
	}
	var client *elastic.Client
	var err error

	for {
		httpClient := http.DefaultClient
		timeout := 60 * time.Second

		if es.config.Timeout > 0 {
			timeout = time.Duration(es.config.Timeout) * time.Second
		}

		log.Printf("[%s] Setting HTTP timeout to %v", es.name, timeout)
		log.Printf("[%s] Setting GZIP enabled: %v", es.name, es.config.GzipEnabled)

		httpClient.Timeout = timeout

		var infoLogger, errorLogger *log.Logger

		if es.config.InfoLogEnabled {
			infoLogger = log.New(os.Stdout, "", log.LstdFlags)
		} else {
			infoLogger = log.New(new(DevNull), "", log.LstdFlags)
		}

		if es.config.ErrorLogEnabled {
			errorLogger = log.New(os.Stderr, "", log.LstdFlags)
		} else {
			errorLogger = log.New(new(DevNull), "", log.LstdFlags)
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

		es.insertIndexTemplate(client)

		break
	}

	log.Printf("Connected to Elasticsearch")

	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, esRecvBuffer)
	es.b.AddSubscriber(es.host, receiveChan)
	defer es.b.DelSubscriber(es.host)

	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	// Create bulk processor
        bulkProcessor, err := client.BulkProcessor().
		After(es.afterCommit).                        // Function to call after commit
		Workers(esWorker).                            // # of workers
		BulkActions(esBulkLimit).                     // # of queued requests before committed
		BulkSize(-1).                                 // No limit
		FlushInterval(esFlushInterval * time.Second). // autocommit every # seconds
		Stats(true).                                  // gather statistics
		Do(context.Background())

        if err != nil {
            log.Println(err)
        }

	idx := &Indexer{bulkProcessor, es.config.IndexPrefix, es.config.IndexType, rateCounter, time.Now()}
	es.idx = idx

	for {

		readInputChannel(*es.config.SampleSize, idx, receiveChan)

		if len(es.term) > 0 {
			select {
			case <-es.term:
				log.Println("Elasticsearch received term signal")
				break
			}
		}
	}

	log.Println("Shutting down. Flushing existing events.")
	defer bulkProcessor.Close()
	return nil
}

func (es *ESServer) Stop() error {
	es.term <- true
	return nil
}
