package main

import (
	"net"
	"net/http"
	"os"
	"io"
	"io/ioutil"
	"fmt"
	"encoding/hex"
	"encoding/json"
	"time"
	"bytes"
	"flag"
	"regexp"
	"crypto/md5"
	"compress/gzip"
	"github.com/ugorji/go/codec"
)

type DummyServerParams struct {
	WriteTimeout time.Duration
	ReadTimeout time.Duration
	ListenOn string
}

var progName = os.Args[0]

func MustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

func Error(fmtStr string, args... interface{}) {
	fmt.Fprint(os.Stderr, progName, ": ")
	fmt.Fprintf(os.Stderr, fmtStr, args...)
	fmt.Fprint(os.Stderr, "\n")
}

func ParseArgs() *DummyServerParams {
	readTimeout := (time.Duration)(0)
	writeTimeout := (time.Duration)(0)
	listenOn := ""

	flagSet := flag.NewFlagSet(progName, flag.ExitOnError)

	flagSet.DurationVar(&readTimeout, "read-timeout", MustParseDuration("10s"), "read timeout on wire")
	flagSet.DurationVar(&writeTimeout, "write-timeout", MustParseDuration("10s"), "write timeout on wire")
	flagSet.StringVar(&listenOn, "listen-on", "127.0.0.1:80", "interface address and port on which the dummy server listens")
	flagSet.Parse(os.Args[1:])

	return &DummyServerParams {
		ReadTimeout: readTimeout,
		WriteTimeout: writeTimeout,
		ListenOn: listenOn,
	}
}

func internalServerError(resp http.ResponseWriter) {
	resp.WriteHeader(500)
	resp.Write([]byte(`{"errorMessage":"Internal Server Error"}`))
}

func handle(resp http.ResponseWriter, req *http.Request, matchparams map[string]string) {
	resp.Header().Set("Content-Type", "application/json; charset=UTF-8")
	h := md5.New()
	format := matchparams["format"]
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		internalServerError(resp)
		return
	}
	rdr := (io.Reader)(bytes.NewReader(body))
	if format == "msgpack.gz" {
		rdr, err = gzip.NewReader(rdr)
		if err != nil {
			internalServerError(resp)
			return
		}
	}
	_codec := &codec.MsgpackHandle{}
	decoder := codec.NewDecoder(rdr, _codec)
	numRecords := 0
	for {
		v := map[string]interface{} {}
		err := decoder.Decode(&v)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				Error("%s", err.Error())
				break
			}
		}
		numRecords += 1
	}
	fmt.Printf("%d records received\n", numRecords)
	io.Copy(h, bytes.NewReader(body))
	md5sum := make([]byte, 0, h.Size())
	md5sum = h.Sum(md5sum)
	uniqueId, _ := matchparams["uniqueId"]
	respData := map[string]interface{} {
		"unique_id": uniqueId,
		"database": matchparams["database"],
		"table": matchparams["table"],
		"md5_hex": hex.EncodeToString(md5sum),
		"elapsed_time": 0.,
	}
	payload, err := json.Marshal(respData)
	if err != nil {
		internalServerError(resp)
		return
	}
	resp.WriteHeader(200)
	resp.Write(payload)
}

type RegexpServeMuxHandler func(http.ResponseWriter, *http.Request, map[string]string)

type regexpServeMuxEntry struct {
	pattern *regexp.Regexp
	handler RegexpServeMuxHandler
}

type RegexpServeMux struct {
	entries []*regexpServeMuxEntry
}

func (mux *RegexpServeMux) Handle(pattern string, handler RegexpServeMuxHandler) error {
	rex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	mux.entries = append(mux.entries, &regexpServeMuxEntry {
		pattern: rex,
		handler: handler,
	})
	return nil
}

func (mux *RegexpServeMux) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	submatches := [][]byte {}
	candidate := (*regexpServeMuxEntry)(nil)
	for _, entry := range mux.entries {
		submatches = entry.pattern.FindSubmatch([]byte(req.URL.Path))
		if submatches != nil {
			candidate = entry
			break
		}
	}
	if candidate == nil {
		resp.WriteHeader(400)
		return
	}
	matchparams := map[string]string {}
	for i, name := range candidate.pattern.SubexpNames() {
		if submatches[i] != nil {
			// XXX: assuming URL is encoded in UTF-8
			matchparams[name] = string(submatches[i])
		}
	}
	candidate.handler(resp, req, matchparams)
}

func newRegexpServeMux () *RegexpServeMux {
	return &RegexpServeMux {
		entries: make([]*regexpServeMuxEntry, 0, 16),
	}
}

func buildMux() *RegexpServeMux {
	mux := newRegexpServeMux()
	err := mux.Handle("^/v3/table/import_with_id/(?P<database>[^/]+)/(?P<table>[^/]+)/(?P<uniqueId>[^/]+)/(?P<format>[^/]+)$", handle)
	if err != nil { panic(err.Error()) }
	err = mux.Handle("^/v3/table/import/(?P<database>[^/]+)/(?P<table>[^/]+)/(?P<format>[^/]+)$", handle)
	if err != nil { panic(err.Error()) }
	return mux
}

var mux = buildMux()

func main() {
	params := ParseArgs()
	server := http.Server {
		Addr: params.ListenOn,
		ReadTimeout: params.ReadTimeout,
		WriteTimeout: params.WriteTimeout,
		Handler: mux,
	}
	listener, err := net.Listen("tcp", params.ListenOn)
	if err != nil {
		Error("%s", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Dummy server listening on %s ...\n", params.ListenOn)
	fmt.Print("Hit CTRL-C to stop\n")
	server.Serve(listener)
}
