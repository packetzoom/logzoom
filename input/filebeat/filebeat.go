package filebeat

import (
	"github.com/packetzoom/logzoom/input"
)

func init() {
	input.Register("filebeat", New)
}
