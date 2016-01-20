package s3

import (
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/packetzoom/logslammer/buffer"
	"github.com/packetzoom/logslammer/output"

	"github.com/jehiah/go-strftime"
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
	TimeSliceFormat string `json:"timeSliceFormat"`
	AwsS3OutputKey  string `json:"awsS3OutputKey"`
}

type FileSaver struct {
	Config   Config
	Writer   *gzip.Writer
	Filename string
}

func (fileSaver *FileSaver) writeToFile(event *buffer.Event) error {
	if fileSaver.Writer == nil {
		log.Println("Creating new S3 gzip writer")
		file, err := ioutil.TempFile(fileSaver.Config.LocalPath, "s3_output_")

		if err != nil {
			log.Printf("Error creating temporary file:", err)
		}

		fileSaver.Writer = gzip.NewWriter(file)
		fileSaver.Filename = file.Name()
	}

	log.Println("Writing data to file")
	text := *event.Text
	_, err := fileSaver.Writer.Write([]byte(text))

	if err != nil {
		log.Println("Error writing:", err)
		return err
	}

	_, err = fileSaver.Writer.Write([]byte("\n"))

	if err != nil {
		log.Println("Error writing:", err)
		return err
	}

	return nil
}

func (s3Writer *S3Writer) uploadToS3(fileSaver *FileSaver) error {
	if fileSaver.Writer == nil {
		return nil
	}

	log.Println("Upload to S3!")
	writer := fileSaver.Writer
	filename := fileSaver.Filename
	fileSaver.Writer = nil
	writer.Close()

	log.Printf("Opening file %s\n", filename)
	reader, err := os.Open(filename)

	if err != nil {
		log.Printf("Failed to open file:", err)
		return err
	}

	curTime := time.Now()
	hostname, _ := os.Hostname()
	timeKey := strftime.Format(s3Writer.Config.TimeSliceFormat, curTime)

	values_for_s3_object_key := map[string]string{
		"path":      s3Writer.Config.Path,
		"timeSlice": timeKey,
		"hostname":  hostname,
		"uuid":      uuid(),
	}

	destFile := s3Writer.Config.AwsS3OutputKey

	for key, value := range values_for_s3_object_key {
		expr := "%{" + key + "}"
		destFile = strings.Replace(destFile, expr, value, -1)
	}

	result, s3Error := s3Writer.S3Uploader.Upload(&s3manager.UploadInput{
		Body:            reader,
		Bucket:          aws.String(s3Writer.Config.AwsS3Bucket),
		Key:             aws.String(destFile),
		ContentEncoding: aws.String("gzip"),
	})

	log.Printf("%s written to S3", result.Location)

	if s3Error == nil {
		os.Remove(filename)
	} else {
		log.Printf("Error uploading to S3", s3Error)
	}

	return s3Error
}

type S3Writer struct {
	Config     Config
	Sender     buffer.Sender
	S3Uploader *s3manager.Uploader
	term       chan bool
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

	aws_access_key_id := s3Writer.Config.AwsKeyId
	aws_secret_access_key := s3Writer.Config.AwsSecKey

	token := ""
	creds := credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, token)
	_, err := creds.Get()

	if err != nil {
		log.Fatalf("Error with AWS credentials:", err)
	}

	session := session.New(&aws.Config{
		Region:      &s3Writer.Config.AwsS3Region,
		Credentials: creds,
	})

	s3Writer.S3Uploader = s3manager.NewUploader(session)
	log.Println("Done instantiating uploader")

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
			go s3Writer.uploadToS3(fileSaver)
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
