package http_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/exergy-dev/twine"
	twhttp "github.com/exergy-dev/twine/http"
)

// A Server routes with a switch on the raw path. The response is whatever
// the RequestCtx holds when the Handler returns.
func ExampleServer() {
	srv := &twhttp.Server{
		Name: "example",
		Handler: func(ctx *twhttp.RequestCtx) {
			switch string(ctx.Path()) { // does not allocate
			case "/hello":
				if !ctx.IsMethod("GET") {
					ctx.SetHeader("Allow", "GET")
					ctx.Error("method not allowed\n", http.StatusMethodNotAllowed)
					return
				}
				ctx.SetHeader("Cache-Control", "no-store")
				ctx.WriteString("hello, ")
				ctx.Write(ctx.Header("X-Name"))
				ctx.WriteString("\n")
			default:
				ctx.SetStatus(http.StatusNotFound)
				ctx.WriteString("not found\n")
			}
		},
	}

	eng := twine.New(twine.Loops(1))
	lst, err := srv.Listen(eng, "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	base := "http://" + lst.Addr().String()
	req, _ := http.NewRequest("GET", base+"/hello", nil)
	req.Header.Set("X-Name", "twine")
	show(http.DefaultClient.Do(req))
	show(http.Post(base+"/hello", "text/plain", nil))
	show(http.Get(base + "/missing"))

	stop()
	if err := <-done; err != nil {
		log.Fatal(err)
	}
	// Output:
	// 200 OK example hello, twine
	// 405 Method Not Allowed example method not allowed
	// 404 Not Found example not found
}

// Adapt serves a net/http.Handler, here a ServeMux with a path pattern, at
// the cost of the usual net/http allocations per request.
func ExampleAdapt() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "item %s\n", r.PathValue("id"))
	})
	srv := &twhttp.Server{Name: "example", Handler: twhttp.Adapt(mux)}

	eng := twine.New(twine.Loops(1))
	lst, err := srv.Listen(eng, "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	base := "http://" + lst.Addr().String()
	show(http.Get(base + "/items/42"))
	show(http.Get(base + "/other"))

	stop()
	if err := <-done; err != nil {
		log.Fatal(err)
	}
	// Output:
	// 200 OK example item 42
	// 404 Not Found example 404 page not found
}

// Error replaces whatever the Handler wrote so far with a plain-text error.
func ExampleRequestCtx_Error() {
	srv := &twhttp.Server{Name: "example", Handler: func(ctx *twhttp.RequestCtx) {
		ctx.WriteString("partial output is discarded")
		if len(ctx.Header("Authorization")) == 0 {
			ctx.Error("unauthorized\n", http.StatusUnauthorized)
			return
		}
		ctx.SetBodyString("welcome\n")
	}}

	eng := twine.New(twine.Loops(1))
	lst, err := srv.Listen(eng, "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	resp, err := http.Get("http://" + lst.Addr().String() + "/")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Header.Get("Content-Type"))
	show(resp, nil)

	stop()
	if err := <-done; err != nil {
		log.Fatal(err)
	}
	// Output:
	// text/plain; charset=utf-8
	// 401 Unauthorized example unauthorized
}

// show prints the status, the Server header and the body of a response.
func show(resp *http.Response, err error) {
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %s %s", resp.Status, resp.Header.Get("Server"), body)
}
