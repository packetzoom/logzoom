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
	"github.com/paulbellamy/ratecounter"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Db         int64  `json:"db"`
	Password   string `json:"password"`
	KeyName    string `json:"keyName"`
	JsonDecode bool   `json:"decode"`
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
	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	for {
		unacked := consumer.GetUnackedLength()

		if unacked > 0 {
			fmt.Printf("Requeued %d messages\n", unacked)
			consumer.RequeueWorking()
		}

		packages, err := consumer.MultiGet(recvBuffer)

		if err == nil {
			numPackages := len(packages)

			if numPackages > 0 {
				rateCounter.Incr(int64(numPackages))
				err = packages[numPackages-1].MultiAck()

				if err != nil {
					log.Println("Failed to ack", err)
				}
			}

			for i := range packages {
				var ev buffer.Event
				payload := string(packages[i].Payload)
				ev.Text = &payload

				if redisServer.config.JsonDecode {
					decoder := json.NewDecoder(strings.NewReader(payload))
					decoder.UseNumber()

					err = decoder.Decode(&ev.Fields)

					if err != nil {
						continue
					}
				}

				redisServer.receiver.Send(&ev)
			}
		} else {
			log.Printf("Error reading from Redis: %s, sleeping", err)
			time.Sleep(2 * time.Second)
		}
	}

	return nil
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

	if err != nil {
		log.Println("Error opening Redis input")
		return err
	}

	go redisGet(redisServer, consumer)

	for {
		select {
		case <-redisServer.term:
			log.Println("Redis input server received term signal")
			return nil
		}
	}

	return nil
}

func (redisServer *RedisInputServer) Stop() error {
	redisServer.term <- true
	return nil
}
