package websocket

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"text/template"
	"time"

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"
	"github.com/packetzoom/logzoom/server"
	"golang.org/x/net/websocket"

	"gopkg.in/yaml.v2"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host string `yaml:"host"`
	SampleSize *int `yaml:"sample_size,omitempty"`
}

type WebSocketServer struct {
	name string
	fields map[string]string
	b    buffer.Sender
	term chan bool
	config *Config

	mtx  sync.RWMutex
	logs map[string]time.Time
}

var (
	indexTemplate, _ = template.New("index").Parse(index)
	logsTemplate, _  = template.New("logs").Parse(logs)
)

func init() {
	output.Register("websocket", New)
}

func New() (output.Output) {
	return &WebSocketServer{
		logs: make(map[string]time.Time),
		term: make(chan bool, 1),
	}
}

func (ws *WebSocketServer) wslogsHandler(w *websocket.Conn) {
	source := w.Request().FormValue("source")
	host := fmt.Sprintf("%s/%d", w.RemoteAddr().String(), time.Now().UnixNano())

	defer func() {
		log.Printf("[%s - %s] closing websocket conn", ws.name, w.RemoteAddr().String())
		ws.b.DelSubscriber(host)
		w.Close()
	}()

	log.Printf("[%s - %s] accepting websocket conn", ws.name, w.RemoteAddr().String())

	r := make(chan *buffer.Event, recvBuffer)
	ws.b.AddSubscriber(host, r)

	for {
		select {
		case ev := <-r:
			if len(source) > 0 {
				if ev.Source != source {
					continue
				}
			}

			if server.RandInt(0, 100) >= *ws.config.SampleSize {
				continue
			}

			err := websocket.Message.Send(w, *ev.Text)
			if err != nil {
				log.Printf("[%s] error sending ws message: %v", w.RemoteAddr().String(), err.Error())
				return
			}
		}
	}
}

func (ws *WebSocketServer) logsHandler(w http.ResponseWriter, r *http.Request) {
	source := "*"
	host := fmt.Sprintf("ws://%s/wslogs", r.Host)

	if src := r.FormValue("source"); len(src) > 0 {
		source = src
		host = fmt.Sprintf("%s?source=%s", host, src)
	}

	logsTemplate.Execute(w, struct{ Source, Server string }{source, host})
}

func (ws *WebSocketServer) indexHandler(w http.ResponseWriter, r *http.Request) {
	ws.mtx.RLock()
	defer ws.mtx.RUnlock()
	indexTemplate.Execute(w, ws.logs)
}

func (ws *WebSocketServer) logListMaintainer() {
	defer func() {
		ws.b.DelSubscriber(ws.name + "_logList")
	}()

	r := make(chan *buffer.Event, recvBuffer)
	ws.b.AddSubscriber(ws.name + "_logList", r)

	ticker := time.NewTicker(time.Duration(600) * time.Second)

	for {
		select {
		case ev := <-r:
			ws.mtx.Lock()
			ws.logs[ev.Source] = time.Now()
			ws.mtx.Unlock()
		case <-ticker.C:
			t := time.Now()
			ws.mtx.Lock()
			for log, ttl := range ws.logs {
				if t.Sub(ttl).Seconds() > 600 {
					delete(ws.logs, log)
				}
			}
			ws.mtx.Unlock()
		}
	}
}

func (ws *WebSocketServer) Init(name string, config yaml.MapSlice, b buffer.Sender, route route.Route) error {
	var wsConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &wsConfig); err != nil {
		return fmt.Errorf("Error parsing websocket config: %v", err)
	}

	ws.name = name
	ws.fields = route.Fields
	ws.config = wsConfig
	ws.b = b
	return nil
}

func (ws *WebSocketServer) Start() error {
	if (ws.b == nil) {
		log.Printf("[%s] No route is specified for this output", ws.name)
		return nil
	}

	if ws.config.SampleSize == nil {
		i := 100
		ws.config.SampleSize = &i
	}
	log.Printf("[%s] Setting Sample Size to %d", ws.name, *ws.config.SampleSize)

	http.Handle("/wslogs", websocket.Handler(ws.wslogsHandler))
	http.HandleFunc("/logs", ws.logsHandler)
	http.HandleFunc("/", ws.indexHandler)

	go ws.logListMaintainer()

	err := http.ListenAndServe(ws.config.Host, nil)
	if err != nil {
		return fmt.Errorf("Error starting websocket server: %v", err)
	}

	return nil
}

func (ws *WebSocketServer) Stop() error {
	ws.term <- true
	return nil
}
