package buffer

import (
	"log"
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
	Fields *map[string]interface{}
}

// subscriber is some host that wants to receive events
type subscriber struct {
	Host string
	Send chan *Event
}

type Buffer struct {
	inputCh     chan *Event
	subscribers map[string]*subscriber
	add         chan *subscriber
	del         chan string
	term        chan bool
	ticker      *time.Ticker
}

func New() *Buffer {
	return &Buffer{
		ticker:      time.NewTicker(time.Duration(10) * time.Millisecond),
		inputCh:     make(chan *Event, bufSize),
		subscribers: make(map[string]*subscriber),
		add:         make(chan *subscriber, 1),
		del:         make(chan string, 1),
		term:        make(chan bool, 1),
	}
}

func (b *Buffer) AddSubscriber(host string, ch chan *Event) error {
	b.add <- &subscriber{host, ch}
	return nil
}

func (b *Buffer) DelSubscriber(host string) error {
	b.del <- host
	return nil
}

func (b *Buffer) Publish(event *Event) {
	for _, sub := range b.subscribers {
		select {
		case sub.Send <- event:
		}
	}
}

func (b *Buffer) InputReceived(event *Event) {
	b.inputCh <- event
}

func (b *Buffer) Start() {
	for {
		select {
		case e := <-b.inputCh:
			b.Publish(e)
		case s := <-b.add:
			if _, ok := b.subscribers[s.Host]; ok {
				log.Printf("A subscriber is already registered for %s\n", s.Host)
				continue
			}
			b.subscribers[s.Host] = s
		case h := <-b.del:
			delete(b.subscribers, h)
		case <-b.term:
			log.Println("Received on term chan")
			break
		case <-b.ticker.C:
		}
	}
}
func (b *Buffer) Stop() {
	b.term <- true
}
