package buffer

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	bufSize = 100
)

type Sender interface {
	AddSubscriber(string, chan *Event) error
	DelSubscriber(string) error
}

// Taken from https://github.com/elasticsearch/logstash-forwarder/blob/master/event.go
type Event struct {
	Source string  `json:"source,omitempty"`
	Offset int64   `json:"offset,omitempty"`
	Line   uint64  `json:"line,omitempty"`
	Text   *string `json:"text,omitempty"`
	Fields *map[string]string
}

// subscriber is some host that wants to receive events
type subscriber struct {
	Host string
	Send chan *Event
}

type Buffer struct {
	mtx         sync.Mutex
	send        chan *Event
	subscribers map[string]*subscriber
	term        chan bool
	ticker      *time.Ticker
}

func New() *Buffer {
	return &Buffer{
		ticker:      time.NewTicker(time.Duration(10) * time.Millisecond),
		send:        make(chan *Event, bufSize),
		subscribers: make(map[string]*subscriber),
		term:        make(chan bool, 1),
	}
}

func (b *Buffer) AddSubscriber(host string, ch chan *Event) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, ok := b.subscribers[host]; ok {
		return fmt.Errorf("subscriber %s already exists", host)
	}

	b.subscribers[host] = &subscriber{host, ch}
	return nil
}

func (b *Buffer) DelSubscriber(host string) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	delete(b.subscribers, host)
	return nil
}

func (b *Buffer) Publish(event *Event) {
	for _, sub := range b.subscribers {
		select {
		case sub.Send <- event:
		case <-b.ticker.C:
		}
	}
}

func (b *Buffer) Send(event *Event) {
	b.send <- event
}

func (b *Buffer) Start() {
	for {
		select {
		case e := <-b.send:
			b.Publish(e)
		case <-b.term:
			log.Println("Received on term chan")
			break
		}
	}
}

func (b *Buffer) Stop() {
	b.term <- true
}
