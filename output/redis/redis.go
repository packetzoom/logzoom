package redis

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adjust/redismq"
	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/server"
	"github.com/packetzoom/logzoom/route"
	"github.com/paulbellamy/ratecounter"

	"gopkg.in/yaml.v2"
)

const (
	redisFlushInterval  = 5
	rateDisplayInterval = 10
	recvBuffer          = 10000
)

type Config struct {
	Host       string   `yaml:"host"`
	Port       int      `yaml:"port"`
	Db         int64    `yaml:"db"`
	Password   string   `yaml:"password"`
	CopyQueues []string `yaml:"copy_queues"`
	SampleSize *int     `yaml:"sample_size,omitempty"`
}

type RedisServer struct {
	name   string
	fields map[string]string
	config Config
	sender buffer.Sender
	term   chan bool
}

type RedisQueue struct {
	queue  *redismq.BufferedQueue
	data   chan string
	term   chan bool
	ticker time.Ticker
}

func NewRedisQueue(config Config, key string) *RedisQueue {
	port := strconv.Itoa(config.Port)

	queue := redismq.CreateBufferedQueue(config.Host,
		port,
		config.Password,
		config.Db,
		key,
		recvBuffer)
	queue.Start()

	return &RedisQueue{queue: queue,
		data:   make(chan string),
		term:   make(chan bool),
		ticker: *time.NewTicker(time.Duration(redisFlushInterval) * time.Second)}
}

func (redisQueue *RedisQueue) insertToRedis(text string) error {
	err := redisQueue.queue.Put(text)

	if err != nil {
		fmt.Println("Error inserting data: ", err)
		return err
	}

	if len(redisQueue.queue.Buffer) > recvBuffer {
		return redisQueue.flushQueue()
	}

	return nil
}

func (redisQueue *RedisQueue) flushQueue() error {
	if len(redisQueue.queue.Buffer) > 0 {
		//	log.Printf("Flushing %d events to Redis", len(redisQueue.queue.Buffer))
	}

	redisQueue.queue.FlushBuffer()
	return nil
}

func (redisQueue *RedisQueue) Start() {
	for {
		select {
		case text := <-redisQueue.data:
			redisQueue.insertToRedis(text)
		case <-redisQueue.ticker.C:
			redisQueue.flushQueue()
		case <-redisQueue.term:
			redisQueue.flushQueue()
		}
	}

}

func init() {
	output.Register("redis", New)
}

func New() output.Output {
	return &RedisServer{term: make(chan bool, 1)}
}

func (redisServer *RedisServer) ValidateConfig(config *Config) error {
	if len(config.Host) == 0 {
		return errors.New("Missing Redis host")
	}

	if config.Port <= 0 {
		return errors.New("Missing Redis port")
	}

	if len(config.CopyQueues) == 0 {
		return errors.New("Missing Redis output queues")
	}

	if redisServer.config.SampleSize == nil {
		i := 100
		redisServer.config.SampleSize = &i
	}
	log.Printf("[%s] Setting Sample Size to %d", redisServer.name, *redisServer.config.SampleSize)

	return nil
}

func (redisServer *RedisServer) Init(name string, config yaml.MapSlice, sender buffer.Sender, route route.Route) error {
	var redisConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &redisConfig); err != nil {
		return fmt.Errorf("Error parsing Redis config: %v", err)
	}

	redisServer.name = name
	redisServer.fields = route.Fields
	redisServer.config = *redisConfig
	redisServer.sender = sender

	if err := redisServer.ValidateConfig(redisConfig); err != nil {
		return fmt.Errorf("Error in config: %v", err)
	}

	return nil
}

func (redisServer *RedisServer) Start() error {
	if (redisServer.sender == nil) {
		log.Printf("[%s] No Route is specified for this output", redisServer.name)
		return nil
	}
	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	redisServer.sender.AddSubscriber(redisServer.name, receiveChan)
	defer redisServer.sender.DelSubscriber(redisServer.name)

	allQueues := make([]*RedisQueue, len(redisServer.config.CopyQueues))

	// Create Redis queue
	for index, key := range redisServer.config.CopyQueues {
		redisQueue := NewRedisQueue(redisServer.config, key)
		allQueues[index] = redisQueue
		go redisQueue.Start()
	}

	log.Printf("[%s] Started Redis Output Instance", redisServer.name)
	// Loop events and publish to Redis
	tick := time.NewTicker(time.Duration(redisFlushInterval) * time.Second)
	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	for {
		select {
		case ev := <-receiveChan:
			rateCounter.Incr(1)
			var allowed bool
			allowed = true
			for key, value :=  range redisServer.fields {
				if ((*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string))) {
					allowed = false
					break
				}
			}
			if allowed && server.RandInt(0, 100) < *redisServer.config.SampleSize {
				text := *ev.Text
				for _, queue := range allQueues {
					queue.data <- text
				}
			}
		case <-tick.C:
			if rateCounter.Rate() > 0 {
				log.Printf("[%s] Current Redis input rate: %d/s\n", redisServer.name, rateCounter.Rate())
			}
		case <-redisServer.term:
			log.Println("RedisServer received term signal")
			for _, queue := range allQueues {
				queue.term <- true
			}

			return nil
		}
	}

	return nil
}

func (s *RedisServer) Stop() error {
	s.term <- true
	return nil
}
