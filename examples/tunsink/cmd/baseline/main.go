// Command baseline is the goroutine-per-connection net.Conn equivalent of
// tunsink, used as the comparison point in the benchmark. Same framing, same
// sink goroutine, same Kafka settings, same -aes decryption (into the sink
// copy, or a per-connection scratch buffer when nothing goes to the sink).
package main

import (
	"bufio"
	"context"
	"crypto/cipher"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/exergy-dev/twine/examples/tunsink/internal/tunpkt"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sys/unix"
)

const maxPacket = 65535

func main() {
	iface := flag.String("tun", "tun0", "TUN interface name (empty: discard)")
	addr := flag.String("addr", ":9000", "TCP listen address")
	brokers := flag.String("brokers", "", "Kafka brokers (empty: disabled)")
	topic := flag.String("topic", "tun-packets", "Kafka topic")
	kafkaDrop := flag.Bool("kafka-drop", false, "drop records when the Kafka client buffer is full instead of blocking (TryProduce vs Produce)")
	mode := flag.String("mode", "channel", "channel: sink goroutine writes TUN; direct: connection goroutine writes TUN")
	sinks := flag.Int("sinks", 1, "sink goroutines")
	statsEvery := flag.Duration("stats", time.Second, "stats interval")
	useAES := flag.Bool("aes", false, "decrypt every frame with AES-256-GCM (nonce || ciphertext || tag)")
	keyHex := flag.String("key", tunpkt.DefaultKeyHex(), "AES-256 key, 64 hex characters")
	flag.Parse()

	var gcm cipher.AEAD
	maxFrame := maxPacket
	if *useAES {
		key, err := tunpkt.ParseKey(*keyHex)
		if err != nil {
			log.Fatal(err)
		}
		if gcm, err = tunpkt.NewAEAD(key); err != nil {
			log.Fatal(err)
		}
		maxFrame = maxPacket + tunpkt.AESOverhead
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tunFD := -1
	if *iface != "" {
		fd, err := tunpkt.OpenTUN(*iface)
		if err != nil {
			log.Fatal(err)
		}
		tunFD = fd
	}
	var kc *kgo.Client
	if *brokers != "" {
		var err error
		kc, err = kgo.NewClient(
			kgo.SeedBrokers(strings.Split(*brokers, ",")...),
			kgo.DefaultProduceTopic(*topic),
			kgo.RequiredAcks(kgo.LeaderAck()),
			kgo.DisableIdempotentWrite(),
			kgo.ProducerLinger(5*time.Millisecond),
			kgo.MaxBufferedRecords(1<<20),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer kc.Close()
	}
	direct := *mode == "direct"

	var frames, bytes, kafkaOK, kafkaErr, tunErr, decryptErr atomic.Uint64
	sink := make(chan []byte, 4096)
	var errOnce sync.Once
	promise := func(_ *kgo.Record, err error) {
		if err != nil {
			kafkaErr.Add(1)
			errOnce.Do(func() { log.Printf("kafka: first produce error: %v", err) })
		} else {
			kafkaOK.Add(1)
		}
	}
	for i := 0; i < *sinks; i++ {
		go func() {
			for pkt := range sink {
				if !direct && tunFD >= 0 {
					if _, err := unix.Write(tunFD, pkt); err != nil {
						tunErr.Add(1)
					}
				}
				if kc != nil {
					if *kafkaDrop {
						kc.TryProduce(context.Background(), &kgo.Record{Value: pkt}, promise)
					} else {
						kc.Produce(context.Background(), &kgo.Record{Value: pkt}, promise)
					}
				}
			}
		}()
	}
	needSink := !direct || kc != nil

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("baseline: tcp%s -> tun=%q kafka=%q mode=%s aes=%v", ln.Addr(), *iface, *brokers, *mode, *useAES)
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	go func() {
		tick := time.NewTicker(*statsEvery)
		defer tick.Stop()
		var lf, lb, lk uint64
		last := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				f, b, k := frames.Load(), bytes.Load(), kafkaOK.Load()
				dt := now.Sub(last).Seconds()
				fmt.Fprintf(os.Stderr, "stats frames/s=%.0f MB/s=%.1f kafka/s=%.0f pauses=0 tun_err=%d kafka_err=%d decrypt_err=%d\n",
					float64(f-lf)/dt, float64(b-lb)/dt/1e6, float64(k-lk)/dt, tunErr.Load(), kafkaErr.Load(), decryptErr.Load())
				lf, lb, lk, last = f, b, k, now
			}
		}
	}()

	var conns sync.WaitGroup
	for {
		c, err := ln.Accept()
		if err != nil {
			break
		}
		conns.Add(1)
		go func() {
			defer conns.Done()
			defer c.Close()
			r := bufio.NewReaderSize(c, 64<<10)
			var hdr [4]byte
			buf := make([]byte, maxFrame)
			var scratch []byte // decrypt buffer when nothing goes to the sink
			if gcm != nil && !needSink {
				scratch = make([]byte, 0, maxPacket)
			}
			for {
				if _, err := io.ReadFull(r, hdr[:]); err != nil {
					return
				}
				n := int(binary.BigEndian.Uint32(hdr[:]))
				if n > maxFrame {
					return
				}
				f := buf[:n]
				if _, err := io.ReadFull(r, f); err != nil {
					return
				}
				frames.Add(1)
				bytes.Add(uint64(n))
				var pkt []byte // owned copy for the sink, set when decrypted into one
				if gcm != nil {
					dst := scratch
					if needSink && n >= tunpkt.AESOverhead {
						dst = make([]byte, 0, n-tunpkt.AESOverhead)
					}
					p, ok := tunpkt.Open(gcm, dst, f)
					if !ok {
						decryptErr.Add(1)
						continue
					}
					f, pkt = p, p
				}
				if direct && tunFD >= 0 {
					if _, err := unix.Write(tunFD, f); err != nil {
						tunErr.Add(1)
					}
				}
				if needSink {
					if gcm == nil {
						pkt = append([]byte(nil), f...)
					}
					sink <- pkt // blocking: TCP backpressure
				}
			}
		}()
	}
	conns.Wait() // connection goroutines may still be blocked on the sink
	close(sink)
	if kc != nil {
		// Records are produced with a background context so shutdown does
		// not fail what is still buffered; flush them first.
		fctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := kc.Flush(fctx); err != nil {
			log.Printf("kafka flush: %v", err)
		}
		cancel()
	}
	log.Printf("total frames=%d bytes=%d tun_err=%d kafka_ok=%d kafka_err=%d decrypt_err=%d",
		frames.Load(), bytes.Load(), tunErr.Load(), kafkaOK.Load(), kafkaErr.Load(), decryptErr.Load())
}
