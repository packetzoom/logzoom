package filebeat

import (
	"github.com/packetzoom/logzoom/input"
)

func init() {
	input.Register("filebeat", &LJServer{
		term: make(chan bool, 1),
	})
}
