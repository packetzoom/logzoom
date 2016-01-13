package parser

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/packetzoom/logslammer/buffer"
)

const (
	ack         = "ACKMSG"
	maxKeyLen   = 100 * 1024 * 1024 // 100 mb
	maxValueLen = 250 * 1024 * 1024 // 250 mb
)

type Receiver interface {
	Send(*buffer.Event)
}

type Parser struct {
	Conn       net.Conn
	Recv       Receiver
	wlen, plen uint32
	buffer     io.Reader
}

func New(c net.Conn, r Receiver) *Parser {
	return &Parser{
		Conn: c,
		Recv: r,
	}
}

// ack acknowledges that the payload was received successfully
func (p *Parser) ack() error {
	if _, err := p.Conn.Write([]byte(ack)); err != nil {
		return err
	}

	return nil
}

// readKV parses key value pairs from within the payload
func (p *Parser) readKV() ([]byte, []byte, error) {
	var klen, vlen uint32

	// Read key len
	binary.Read(p.buffer, binary.BigEndian, &klen)

	if klen > maxKeyLen {
		return nil, nil, fmt.Errorf("key exceeds max len %d, got %d bytes", maxKeyLen, klen)
	}

	// Read key
	key := make([]byte, klen)
	_, err := p.buffer.Read(key)
	if err != nil {
		return nil, nil, err
	}

	// Read value len
	binary.Read(p.buffer, binary.BigEndian, &vlen)
	if vlen > maxValueLen {
		return nil, nil, fmt.Errorf("value exceeds max len %d, got %d bytes", maxValueLen, vlen)
	}

	// Read value
	value := make([]byte, vlen)
	_, err = p.buffer.Read(value)
	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

// read parses the compressed data frame
func (p *Parser) read() error {
	var s, f uint32
	var k, v []byte
	var err error

	r, err := zlib.NewReader(p.Conn)
	if err != nil {
		return err
	}
	defer r.Close()

	// Decompress
	buff := new(bytes.Buffer)
	io.Copy(buff, r)
	p.buffer = buff

	b := make([]byte, 2)
	for i := uint32(0); i < p.wlen; i++ {
		n, err := buff.Read(b)
		if err == io.EOF {
			return err
		}

		if n == 0 {
			continue
		}

		switch string(b) {
		case "1D": // window size
			binary.Read(buff, binary.BigEndian, &s)
			binary.Read(buff, binary.BigEndian, &f)

			var ev buffer.Event
			fields := make(map[string]string)
			fields["timestamp"] = time.Now().Format(time.RFC3339Nano)

			for j := uint32(0); j < f; j++ {
				if k, v, err = p.readKV(); err != nil {
					return err
				}
				fields[string(k)] = string(v)
			}

			ev.Source = fmt.Sprintf("lumberjack://%s%s", fields["host"], fields["file"])
			ev.Offset, _ = strconv.ParseInt(fields["offset"], 10, 64)
			ev.Line = uint64(s)
			t := fields["line"]
			ev.Text = &t
			ev.Fields = &fields

			// Send to the receiver which is a buffer. We block because...
			p.Recv.Send(&ev)
		default:
			return fmt.Errorf("unknown type")
		}
	}

	return nil
}

// Parse initialises the read loop and begins parsing the incoming request
func (p *Parser) Parse() {
	b := make([]byte, 2)

Read:
	for {
		n, err := p.Conn.Read(b)
		switch {
		case err == io.EOF:
			break Read
		case n == 0:
			goto Read
		}

		switch string(b) {
		case "1W": // window length
			binary.Read(p.Conn, binary.BigEndian, &p.wlen)
		case "1C": // frame length
			binary.Read(p.Conn, binary.BigEndian, &p.plen)
			if err := p.read(); err != nil {
				log.Printf("[%s] error parsing %v", p.Conn.RemoteAddr().String(), err)
				break Read
			}

			if err := p.ack(); err != nil {
				log.Printf("[%s] error acking %v", p.Conn.RemoteAddr().String(), err)
				break Read
			}
		default:
			// This really shouldn't happen
			log.Printf("[%s] Received unknown type", p.Conn.RemoteAddr().String(), err)
			break Read
		}
	}
}
