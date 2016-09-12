package s3

import (
	"compress/gzip"
	"crypto/rand"
	"errors"
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

	"github.com/packetzoom/logzoom/buffer"
	"github.com/packetzoom/logzoom/output"
	"github.com/packetzoom/logzoom/route"

	"github.com/jehiah/go-strftime"
	"github.com/paulbellamy/ratecounter"

	"gopkg.in/yaml.v2"
)

const (
	s3FlushInterval        = 10
	recvBuffer             = 100
	maxSimultaneousUploads = 8
)

func uuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Config struct {
	AwsKeyIdLoc  string `yaml:"aws_key_id_loc"`
	AwsSecKeyLoc string `yaml:"aws_sec_key_loc"`
	AwsS3Bucket  string `yaml:"aws_s3_bucket"`
	AwsS3Region  string `yaml:"aws_s3_region"`

	LocalPath       string `yaml:"local_path"`
	Path            string `yaml:"s3_path"`
	TimeSliceFormat string `yaml:"time_slice_format"`
	AwsS3OutputKey  string `yaml:"aws_s3_output_key"`
}

type OutputFileInfo struct {
	Filename string
	Count    int
}

type FileSaver struct {
	Config      Config
	Writer      *gzip.Writer
	FileInfo    OutputFileInfo
	RateCounter *ratecounter.RateCounter
}

func (fileSaver *FileSaver) WriteToFile(name string, event *buffer.Event) error {
	if fileSaver.Writer == nil {
		log.Println("Creating new S3 gzip writer")
		file, err := ioutil.TempFile(fileSaver.Config.LocalPath, name)

		if err != nil {
			log.Printf("Error creating temporary file:", err)
		}

		fileSaver.Writer = gzip.NewWriter(file)
		fileSaver.FileInfo.Filename = file.Name()
		fileSaver.FileInfo.Count = 0
	}

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

	fileSaver.FileInfo.Count += 1
	fileSaver.RateCounter.Incr(1)

	return nil
}

func (s3Writer *S3Writer) doUpload(fileInfo OutputFileInfo) error {
	log.Printf("Opening file %s\n", fileInfo.Filename)
	reader, err := os.Open(fileInfo.Filename)

	if err != nil {
		log.Printf("Failed to open file:", err)
		return err
	}

	curTime := time.Now()
	hostname, _ := os.Hostname()
	timeKey := strftime.Format(s3Writer.Config.TimeSliceFormat, curTime)

	valuesForKey := map[string]string{
		"path":      s3Writer.Config.Path,
		"timeSlice": timeKey,
		"hostname":  hostname,
		"uuid":      uuid(),
	}

	destFile := s3Writer.Config.AwsS3OutputKey

	for key, value := range valuesForKey {
		expr := "%{" + key + "}"
		destFile = strings.Replace(destFile, expr, value, -1)
	}

	result, s3Error := s3Writer.S3Uploader.Upload(&s3manager.UploadInput{
		Body:            reader,
		Bucket:          aws.String(s3Writer.Config.AwsS3Bucket),
		Key:             aws.String(destFile),
		ContentEncoding: aws.String("gzip"),
	})

	if s3Error == nil {
		log.Printf("%d events written to S3 %s", fileInfo.Count, result.Location)
		os.Remove(fileInfo.Filename)
	} else {
		log.Printf("Error uploading to S3", s3Error)
	}

	return s3Error

}

func (s3Writer *S3Writer) WaitForUpload() {
	for {
		select {
		case fileInfo := <-s3Writer.uploadChannel:
			s3Writer.doUpload(fileInfo)
		}
	}
}

func (s3Writer *S3Writer) InitiateUploadToS3(fileSaver *FileSaver) {
	if fileSaver.Writer == nil {
		return
	}

	log.Printf("Upload to S3, current event rate: %d/s\n", fileSaver.RateCounter.Rate())
	writer := fileSaver.Writer
	fileInfo := fileSaver.FileInfo
	fileSaver.Writer = nil
	writer.Close()

	s3Writer.uploadChannel <- fileInfo
}

type S3Writer struct {
	name          string
	fields        map[string]string
	Config        Config
	Sender        buffer.Sender
	S3Uploader    *s3manager.Uploader
	uploadChannel chan OutputFileInfo
	term          chan bool
}

func init() {
	output.Register("s3", New)
}

func New() output.Output {
	return &S3Writer{term: make(chan bool, 1)}
}

func (s3Writer *S3Writer) ValidateConfig(config *Config) error {
	if len(config.LocalPath) == 0 {
		return errors.New("missing local path")
	}

	// Create the local path if necessary
	if err := os.MkdirAll(config.LocalPath, 0700); err != nil {
		return errors.New("could not mkdir " + config.LocalPath)
	}

	// Try writing to local path
	if _, err := ioutil.TempFile(config.LocalPath, "logzoom"); err != nil {
		return errors.New("unable to write to " + config.LocalPath)
	}

	if len(config.AwsS3Bucket) == 0 {
		return errors.New("missing AWS S3 bucket")
	}

	if len(config.AwsS3Region) == 0 {
		return errors.New("missing AWS S3 region")
	}

	if len(config.AwsS3OutputKey) == 0 {
		return errors.New("missing AWS S3 output key")
	}

	return nil
}

func (s3Writer *S3Writer) Init(name string, config yaml.MapSlice, sender buffer.Sender, route route.Route) error {
	var s3Config *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &s3Config); err != nil {
		return fmt.Errorf("Error parsing S3 config: %v", err)
	}

	if err := s3Writer.ValidateConfig(s3Config); err != nil {
		return fmt.Errorf("Error in config: %v", err)
	}

	s3Writer.name = name
	s3Writer.fields = route.Fields
	s3Writer.uploadChannel = make(chan OutputFileInfo, maxSimultaneousUploads)
	s3Writer.Config = *s3Config
	s3Writer.Sender = sender
	aws_access_key_id_data, error := ioutil.ReadFile(s3Writer.Config.AwsKeyIdLoc)
	aws_access_key_id := strings.TrimSpace(string(aws_access_key_id_data))
	if error != nil {
		return fmt.Errorf("AWS Access Key ID not found: %v", error)
	}
	aws_secret_access_key_data, error := ioutil.ReadFile(s3Writer.Config.AwsSecKeyLoc)
	aws_secret_access_key := strings.TrimSpace(string(aws_secret_access_key_data))
	if error != nil {
		return fmt.Errorf("AWS Secret Key not found: %v", error)
	}
	token := ""
	creds := credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, token)
	_, err := creds.Get()

	if err != nil {
		return err
	}

	session := session.New(&aws.Config{
		Region:      &s3Writer.Config.AwsS3Region,
		Credentials: creds,
	})

	s3Writer.S3Uploader = s3manager.NewUploader(session)
	log.Println("Done instantiating S3 uploader")

	return nil
}

func (s3Writer *S3Writer) Start() error {
	if s3Writer.Sender == nil {
		log.Printf("[%s] No route is specified for this output", s3Writer.name)
		return nil
	}
	// Create file saver
	fileSaver := new(FileSaver)
	fileSaver.Config = s3Writer.Config
	fileSaver.RateCounter = ratecounter.NewRateCounter(1 * time.Second)
	id := "s3_output"
	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, recvBuffer)
	s3Writer.Sender.AddSubscriber(id, receiveChan)
	defer s3Writer.Sender.DelSubscriber(id)

	// Loop events and publish to S3
	tick := time.NewTicker(time.Duration(s3FlushInterval) * time.Second)

	go s3Writer.WaitForUpload()

	for {
		select {
		case ev := <-receiveChan:
			var allowed bool
			allowed = true
			for key, value := range s3Writer.fields {
				if (*ev.Fields)[key] == nil || ((*ev.Fields)[key] != nil && value != (*ev.Fields)[key].(string)) {
					allowed = false
					break
				}
			}
			if allowed {
				fileSaver.WriteToFile(s3Writer.name, ev)
			}
		case <-tick.C:
			s3Writer.InitiateUploadToS3(fileSaver)
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
