//go:build linux

// Command fakekafka runs an in-process franz-go kfake broker so tunsink can
// be benchmarked without a Kafka installation.
//
//	go run ./cmd/fakekafka -port 9092 -topic tun-packets
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kfake"
)

func main() {
	port := flag.Int("port", 9092, "listen port on 127.0.0.1")
	topic := flag.String("topic", "tun-packets", "topic to pre-create")
	parts := flag.Int("partitions", 4, "partitions for the topic")
	flag.Parse()

	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.Ports(*port),
		kfake.SeedTopics(int32(*parts), *topic),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	log.Printf("fakekafka: %v topic=%s partitions=%d", c.ListenAddrs(), *topic, *parts)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}
