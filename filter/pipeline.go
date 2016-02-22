package filter

import (
	"github.com/packetzoom/logslammer/buffer"
	"log"
)

type Pipeline struct {
	head     chan *buffer.Event
	tail     chan *buffer.Event
	NumPipes int
}

func (p *Pipeline) Enqueue(event *buffer.Event) {
	p.head <- event
}

func (p *Pipeline) Dequeue(handler func(*buffer.Event)) {
	for i := range p.tail {
		log.Println("dequeu here")
		handler(i)
	}
}

func (p *Pipeline) Close() {
	close(p.head)
}

func NewPipeline(pipes ...Pipe) *Pipeline {
	head := make(chan *buffer.Event)
	var nextChan chan *buffer.Event

	for _, pipe := range pipes {
		if nextChan == nil {
			nextChan = pipe.Process(head)
		} else {
			nextChan = pipe.Process(nextChan)
		}
	}

	return &Pipeline{head: head, tail: nextChan, NumPipes: len(pipes)}
}
