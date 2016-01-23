package redis

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adjust/redismq"
	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"
	"github.com/paulbellamy/ratecounter"
)

const (
	redisFlushInterval  = 5
	rateDisplayInterval = 10
	recvBuffer          = 100
)

type Config struct {
	Host     string   `json:"host"`
	Port     int      `json:"port"`
	Db       int64    `json:"db"`
	Password string   `json:"password"`
	Keys     []string `json:"keys"`
}

type RedisServer struct {
	config Config
	sender buffer.Sender
	term   chan bool
}

func init() {
	output.Register("redis", &RedisServer{
		term: make(chan bool, 1),
	})
}

func (redisServer *RedisServer) Init(config json.RawMessage, sender buffer.Sender) error {
	var redisConfig *Config
	if err := json.Unmarshal(config, &redisConfig); err != nil {
		return fmt.Errorf("Error parsing Redis config: %v", err)
	}

	redisServer.config = *redisConfig
	redisServer.sender = sender
	return nil
}

func insertToRedis(queue *redismq.BufferedQueue, ev *buffer.Event) error {
	text := *ev.Text
	err := queue.Put(text)

	if err != nil {
		fmt.Println("Error inserting data: ", err)
		return err
	}

	if len(queue.Buffer) > recvBuffer {
		return flushQueue(queue)
	}

	return nil
}

func flushQueue(queue *redismq.BufferedQueue) error {
	if len(queue.Buffer) > 0 {
		//	log.Printf("Flushing %d events to Redis", len(queue.Buffer))
	}

	queue.FlushBuffer()
	return nil
}

func (redisServer *RedisServer) Start() error {
	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	redisServer.sender.AddSubscriber(redisServer.config.Host, receiveChan)
	defer redisServer.sender.DelSubscriber(redisServer.config.Host)

	port := strconv.Itoa(redisServer.config.Port)

	allQueues := make([]*redismq.BufferedQueue, len(redisServer.config.Keys))

	// Create Redis queue
	for index, key := range redisServer.config.Keys {
		queue := redismq.CreateBufferedQueue(redisServer.config.Host,
			port,
			redisServer.config.Password,
			redisServer.config.Db,
			key,
			recvBuffer)
		queue.Start()
		allQueues[index] = queue
	}

	// Loop events and publish to Redis
	tick := time.NewTicker(time.Duration(redisFlushInterval) * time.Second)
	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	for {
		select {
		case ev := <-receiveChan:
			rateCounter.Incr(1)
			for _, queue := range allQueues {
				go insertToRedis(queue, ev)
			}
		case <-tick.C:
			if rateConter.Rate() > 0 {
				log.Printf("Current Redis input rate: %d/s\n", rateCounter.Rate())
			}

			for _, queue := range allQueues {
				go flushQueue(queue)
			}
		case <-redisServer.term:
			log.Println("RedisServer received term signal")
			return nil
		}
	}

	return nil
}

func (s *RedisServer) Stop() error {
	s.term <- true
	return nil
}
