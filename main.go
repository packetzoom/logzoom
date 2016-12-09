package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/pprof"

	_ "github.com/packetzoom/logzoom/input/filebeat"
	_ "github.com/packetzoom/logzoom/input/redis"
	_ "github.com/packetzoom/logzoom/input/stdin"
	_ "github.com/packetzoom/logzoom/output/elasticsearch"
	_ "github.com/packetzoom/logzoom/output/redis"
	_ "github.com/packetzoom/logzoom/output/s3"
	_ "github.com/packetzoom/logzoom/output/tcp"
	_ "github.com/packetzoom/logzoom/output/websocket"
	"github.com/packetzoom/logzoom/server"
)

var (
	config                           string
	memprofile, cpuprofile, httpprof *string
)

func init() {
	memprofile = flag.String("memprofile", "", "Write memory profile to this file")
	cpuprofile = flag.String("cpuprofile", "", "Write cpu profile to file")
	httpprof = flag.String("httpprof", "", "Start pprof http server")
	flag.StringVar(&config, "config", "", "Path to the config file")
	flag.Parse()

	if len(config) == 0 {
		fmt.Fprintln(os.Stderr, "Require a config file")
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func writeHeapProfile(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed creating file %s: %s\n", filename, err)
		return
	}
	pprof.WriteHeapProfile(f)
	f.Close()

	log.Printf("Created memory profile file %s.\n", filename)
}

func Cleanup() {
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		runtime.GC()

		writeHeapProfile(*memprofile)

		debugMemStats()
	}
}

func debugMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Memory stats: In use: %d Total (even if freed): %d System: %d\n",
		m.Alloc, m.TotalAlloc, m.Sys)
}

func BeforeRun() {
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	if *httpprof != "" {
		go func() {
			log.Println("start pprof endpoint")
			log.Printf("finished pprof endpoint: %v\n", http.ListenAndServe(*httpprof, nil))
		}()
	}
}

func main() {
	defer Cleanup()

	BeforeRun()

	srv, err := server.New(config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	srv.Start()
}
