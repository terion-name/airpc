// Package grpcobs passively decodes gRPC-over-HTTP/2 relayed on grpc routes
// to record per-method RPC metrics (method, grpc-status, duration).
//
// It observes copies of the relayed bytes and never blocks or modifies
// traffic: bytes are handed to the parsers through bounded queues, and when a
// queue overflows or a connection uses HTTP/2 features the parser cannot
// follow, observation for that connection goes dark while the relay continues
// untouched.
package grpcobs

import (
	"bytes"
	"io"
	"regexp"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/terion-name/airpc/internal/telemetry"
)

// methodPattern bounds metric label cardinality to sane gRPC method names.
var methodPattern = regexp.MustCompile(`^/[A-Za-z0-9_.]{1,200}/[A-Za-z0-9_]{1,100}$`)

// Observer watches one relayed gRPC connection.
type Observer struct {
	route   string
	done    chan struct{}
	client  *feed
	backend *feed

	mu      sync.Mutex
	started map[uint32]rpcStart // h2 stream id -> in-flight RPC
}

type rpcStart struct {
	method string
	start  time.Time
}

func New(route string) *Observer {
	o := &Observer{
		route:   route,
		done:    make(chan struct{}),
		client:  newFeed(),
		backend: newFeed(),
		started: make(map[uint32]rpcStart),
	}
	go o.parseClient()
	go o.parseBackend()
	return o
}

// ClientBytes observes bytes flowing from the public client to the backend.
// It must be called from a single goroutine and never blocks.
func (o *Observer) ClientBytes(p []byte) { o.client.put(p) }

// BackendBytes observes bytes flowing from the backend to the public client.
// It must be called from a single goroutine and never blocks.
func (o *Observer) BackendBytes(p []byte) { o.backend.put(p) }

// Close releases the parser goroutines. The connection is finished; RPCs
// without observed trailers are not recorded.
func (o *Observer) Close() { close(o.done) }

func (o *Observer) parseClient() {
	reader := &feedReader{feed: o.client, done: o.done}
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(reader, preface); err != nil || !bytes.Equal(preface, []byte(http2.ClientPreface)) {
		return
	}
	framer := newFramer(reader)
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			return
		}
		headers, ok := frame.(*http2.MetaHeadersFrame)
		if !ok {
			continue
		}
		method := headers.PseudoValue("path")
		if !methodPattern.MatchString(method) {
			method = "other"
		}
		o.mu.Lock()
		o.started[headers.StreamID] = rpcStart{method: method, start: time.Now()}
		o.mu.Unlock()
	}
}

func (o *Observer) parseBackend() {
	framer := newFramer(&feedReader{feed: o.backend, done: o.done})
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			return
		}
		switch f := frame.(type) {
		case *http2.MetaHeadersFrame:
			// grpc-status appears in trailers (or headers of a
			// trailers-only response); its arrival ends the RPC.
			for _, field := range f.Fields {
				if field.Name == "grpc-status" {
					o.finish(f.StreamID, statusCode(field.Value))
					break
				}
			}
		case *http2.RSTStreamFrame:
			o.finish(f.StreamID, "canceled")
		}
	}
}

func (o *Observer) finish(streamID uint32, code string) {
	o.mu.Lock()
	rpc, ok := o.started[streamID]
	delete(o.started, streamID)
	o.mu.Unlock()
	if !ok {
		return
	}
	telemetry.GRPCRPCs.WithLabelValues(o.route, rpc.method, code).Inc()
	telemetry.GRPCDuration.WithLabelValues(o.route, rpc.method).Observe(time.Since(rpc.start).Seconds())
}

func statusCode(value string) string {
	if n, err := strconv.Atoi(value); err == nil && n >= 0 && n <= 16 {
		return value
	}
	return "invalid"
}

func newFramer(r io.Reader) *http2.Framer {
	framer := http2.NewFramer(io.Discard, r)
	framer.SetMaxReadFrameSize(16 << 20)
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	return framer
}

// feedQueueSize bounds per-direction buffering; a full queue marks the feed
// dead rather than backpressuring the relay.
const feedQueueSize = 256

type feed struct {
	ch   chan []byte
	dead bool // only the single producer goroutine touches it
}

func newFeed() *feed {
	return &feed{ch: make(chan []byte, feedQueueSize)}
}

func (f *feed) put(p []byte) {
	if f.dead {
		return
	}
	buf := append([]byte(nil), p...)
	select {
	case f.ch <- buf:
	default:
		f.dead = true
	}
}

// feedReader adapts a feed to io.Reader for the framer; it returns EOF once
// the observer is closed.
type feedReader struct {
	feed *feed
	done <-chan struct{}
	rest []byte
}

func (r *feedReader) Read(p []byte) (int, error) {
	for len(r.rest) == 0 {
		select {
		case chunk := <-r.feed.ch:
			r.rest = chunk
		case <-r.done:
			// Drain anything already queued before reporting EOF.
			select {
			case chunk := <-r.feed.ch:
				r.rest = chunk
			default:
				return 0, io.EOF
			}
		}
	}
	n := copy(p, r.rest)
	r.rest = r.rest[n:]
	return n, nil
}
