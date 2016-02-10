package websocket

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"text/template"
	"time"

	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"
	"golang.org/x/net/websocket"

	"gopkg.in/yaml.v2"
)

const (
	recvBuffer = 100
)

type Config struct {
	Host string `yaml:"host"`
}

type WebSocketServer struct {
	host string
	b    buffer.Sender
	term chan bool

	mtx  sync.RWMutex
	logs map[string]time.Time
}

var (
	indexTemplate, _ = template.New("index").Parse(index)
	logsTemplate, _  = template.New("logs").Parse(logs)
)

func init() {
	output.Register("websocket", &WebSocketServer{
		logs: make(map[string]time.Time),
		term: make(chan bool, 1),
	})
}

func (ws *WebSocketServer) wslogsHandler(w *websocket.Conn) {
	source := w.Request().FormValue("source")
	host := fmt.Sprintf("%s/%d", w.RemoteAddr().String(), time.Now().UnixNano())

	defer func() {
		log.Printf("[%s] closing websocket conn", w.RemoteAddr().String())
		ws.b.DelSubscriber(host)
		w.Close()
	}()

	log.Printf("[%s] accepting websocket conn", w.RemoteAddr().String())

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
		ws.b.DelSubscriber("logList")
	}()

	r := make(chan *buffer.Event, recvBuffer)
	ws.b.AddSubscriber("logList", r)

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

func (ws *WebSocketServer) Init(config yaml.MapSlice, b buffer.Sender) error {
	var wsConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &wsConfig); err != nil {
		return fmt.Errorf("Error parsing websocket config: %v", err)
	}

	ws.host = wsConfig.Host
	ws.b = b
	return nil
}

func (ws *WebSocketServer) Start() error {
	http.Handle("/wslogs", websocket.Handler(ws.wslogsHandler))
	http.HandleFunc("/logs", ws.logsHandler)
	http.HandleFunc("/", ws.indexHandler)

	go ws.logListMaintainer()

	err := http.ListenAndServe(ws.host, nil)
	if err != nil {
		return fmt.Errorf("Error starting websocket server: %v", err)
	}

	return nil
}

func (ws *WebSocketServer) Stop() error {
	ws.term <- true
	return nil
}
