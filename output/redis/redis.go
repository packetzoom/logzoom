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
)

const (
	redisFlushInterval = 5
	recvBuffer         = 100
)

type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Db       int64  `json:"db"`
	Password string `json:"password"`
	KeyName  string `json:"keyName"`
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
	log.Println("Inserting to redis!")
	text := *ev.Text
	return queue.Put(text)
}

func flushQueue(queue *redismq.BufferedQueue) error {
	queue.FlushBuffer()
	return nil
}

func (redisServer *RedisServer) Start() error {
	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	redisServer.sender.AddSubscriber(redisServer.config.Host, receiveChan)
	defer redisServer.sender.DelSubscriber(redisServer.config.Host)

	port := strconv.Itoa(redisServer.config.Port)

	// Create Redis queue
	queue := redismq.CreateBufferedQueue(redisServer.config.Host,
		port,
		redisServer.config.Password,
		redisServer.config.Db,
		redisServer.config.KeyName,
		recvBuffer)
	queue.Start()

	// Loop events and publish to Redis
	tick := time.NewTicker(time.Duration(redisFlushInterval) * time.Second)

	for {
		select {
		case ev := <-receiveChan:
			go insertToRedis(queue, ev)
		case <-tick.C:
			go flushQueue(queue)
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
