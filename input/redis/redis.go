package redis

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/adjust/redismq"
	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/input"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Db       int64  `json:"db"`
	Password string `json:"password"`
	KeyName  string `json:"keyName"`
}

type RedisInputServer struct {
	config   Config
	receiver input.Receiver
	term     chan bool
}

func init() {
	input.Register("redis", &RedisInputServer{
		term: make(chan bool, 1),
	})
}

func redisGet(redisServer *RedisInputServer, consumer *redismq.Consumer) error {
	consumer.ResetWorking()
	packages, err := consumer.MultiGet(recvBuffer)

	if err == nil {
		var ev buffer.Event

		for i := range packages {
			packages[i].Ack()

			ev.Text = &packages[i].Payload
			decoder := json.NewDecoder(strings.NewReader(string(*ev.Text)))
			decoder.UseNumber()

			err = decoder.Decode(&ev.Fields)

			if err == nil {
				redisServer.receiver.Send(&ev)
			}
		}
	}

	return err
}

func (redisServer *RedisInputServer) Init(config json.RawMessage, receiver input.Receiver) error {
	var redisConfig *Config
	if err := json.Unmarshal(config, &redisConfig); err != nil {
		return fmt.Errorf("Error parsing Redis config: %v", err)
	}

	redisServer.config = *redisConfig
	redisServer.receiver = receiver

	return nil
}

func (redisServer *RedisInputServer) Start() error {
	log.Println("Starting Redis input")
	port := strconv.Itoa(redisServer.config.Port)

	// Create Redis queue
	queue := redismq.CreateQueue(redisServer.config.Host,
		port,
		redisServer.config.Password,
		redisServer.config.Db,
		redisServer.config.KeyName)

	consumer, err := queue.AddConsumer("redisInput")

	// Read from Redis every second
	tick := time.NewTicker(time.Duration(1) * time.Second)

	if err != nil {
		log.Println("Error opening Redis input")
		return err
	}

	for {
		select {
		case <-redisServer.term:
			log.Println("Redis input server received term signal")
			return nil
		case <-tick.C:
			go redisGet(redisServer, consumer)
		}
	}

	return nil
}

func (redisServer *RedisInputServer) Stop() error {
	redisServer.term <- true
	return nil
}
