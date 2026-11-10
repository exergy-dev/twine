//go:build linux

package bench

import (
	"context"
	"net"
	nethttp "net/http"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/twine"
	twhttp "github.com/exergy-dev/twine/http"
)

// HelloBody is the plaintext response body (the TechEmpower "plaintext"
// test).
const HelloBody = "Hello, World!"

var helloBytes = []byte(HelloBody)

// StartHTTPTwine starts a twine/http hello-world server with loops event
// loops (0 means GOMAXPROCS). The returned server's Conns counts open
// connections.
func StartHTTPTwine(addr string, loops int, opts ...twine.Option) (*TwineServer, error) {
	srv := &twhttp.Server{Handler: func(ctx *twhttp.RequestCtx) {
		ctx.SetContentType("text/plain")
		ctx.SetBody(helloBytes)
	}}
	return startTwine(addr, loops, srv.Factory(), opts...)
}

// NetHTTPServer is the net/http hello-world server.
type NetHTTPServer struct {
	srv  *nethttp.Server
	ln   net.Listener
	n    atomic.Int64
	done chan struct{}
}

// StartHTTPNet starts a net/http hello-world server on addr.
func StartHTTPNet(addr string) (*NetHTTPServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &NetHTTPServer{ln: ln, done: make(chan struct{})}
	s.srv = &nethttp.Server{
		Handler: nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write(helloBytes)
		}),
		ConnState: func(_ net.Conn, st nethttp.ConnState) {
			switch st {
			case nethttp.StateNew:
				s.n.Add(1)
			case nethttp.StateClosed, nethttp.StateHijacked:
				s.n.Add(-1)
			}
		},
	}
	go func() {
		defer close(s.done)
		_ = s.srv.Serve(ln)
	}()
	return s, nil
}

// Addr implements Server.
func (s *NetHTTPServer) Addr() string { return s.ln.Addr().String() }

// Conns implements Server.
func (s *NetHTTPServer) Conns() int { return int(s.n.Load()) }

// Close implements Server.
func (s *NetHTTPServer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.srv.Shutdown(ctx) != nil {
		_ = s.srv.Close()
	}
	<-s.done
}
