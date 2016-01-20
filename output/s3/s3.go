package s3

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"time"

	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"
)

const (
	s3FlushInterval = 5
	recvBuffer      = 100
)

func uuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Config struct {
	AwsKeyId    string `json:"awsKeyId"`
	AwsSecKey   string `json:"awsSecKey"`
	AwsS3Bucket string `json:"awsS3Bucket"`
	AwsS3Region string `json:"awsS3Region"`

	LocalPath       string `json:"localPath"`
	Path            string `json:"path"`
	TimeSliceFormat string `json:"path"`
	AwsS3OutputKey  string `json:"awsS3OutputKey"`
}

type FileSaver struct {
	Config Config
	Buffer bytes.Buffer
	Writer *gzip.Writer
}

func (fileSaver *FileSaver) writeToFile(event *buffer.Event) error {
	if fileSaver.Writer == nil {
		fileSaver.Writer = gzip.NewWriter(&fileSaver.Buffer)
	}

	text := *event.Text
	fileSaver.Writer.Write([]byte(text))
	fileSaver.Writer.Write([]byte("\n"))
	return nil
}

func (fileSaver *FileSaver) flushToS3() error {
	if fileSaver.Buffer.Len() > 0 {
		file, err := ioutil.TempFile(fileSaver.Config.LocalPath, "s3_output_")

		if err != nil {
			log.Printf("Error creating temporary file: %s", filename)
		}

		log.Printf("Flushing %s", file.Name())
		fileSaver.Writer.Close()
		fileSaver.Writer = nil
		err = ioutil.WriteFile(file.Name(), fileSaver.Buffer.Bytes(), 0666)

		if err == nil {
			fileSaver.Buffer.Reset()
		} else {
			log.Printf("Error saving file: %s", err)
			return err
		}
	}

	return nil
}

type S3Writer struct {
	Config Config
	Sender buffer.Sender
	term   chan bool
}

func init() {
	output.Register("s3", &S3Writer{
		term: make(chan bool, 1),
	})
}

func (s3Writer *S3Writer) Init(config json.RawMessage, sender buffer.Sender) error {
	var s3Config *Config
	if err := json.Unmarshal(config, &s3Config); err != nil {
		return fmt.Errorf("Error parsing S3 config: %v", err)
	}

	s3Writer.Config = *s3Config
	s3Writer.Sender = sender
	return nil
}

func insertToS3(ev *buffer.Event) error {
	log.Println("Inserting to s3!")

	return nil
}

func (s3Writer *S3Writer) Start() error {
	// Create file saver
	fileSaver := new(FileSaver)
	fileSaver.Config = s3Writer.Config

	id := "s3_output"
	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	s3Writer.Sender.AddSubscriber(id, receiveChan)
	defer s3Writer.Sender.DelSubscriber(id)

	// Loop events and publish to S3
	tick := time.NewTicker(time.Duration(s3FlushInterval) * time.Second)

	for {
		select {
		case ev := <-receiveChan:
			fileSaver.writeToFile(ev)
		case <-tick.C:
			fileSaver.flushToS3()
		case <-s3Writer.term:
			log.Println("S3Writer received term signal")
			return nil
		}
	}

	return nil
}

func (s *S3Writer) Stop() error {
	s.term <- true
	return nil
}
