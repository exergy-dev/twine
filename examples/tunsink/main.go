// Command tunsink accepts TCP clients and writes every length-prefixed frame
// they send into a TUN device and onto a Kafka topic.
//
// Each frame is a 4-byte big-endian length followed by one IP packet
// (proto.LengthPrefixed defaults). Two sink modes exist:
//
//	-mode channel  frames are copied once and handed to a goroutine that
//	               writes the TUN and produces to Kafka (nothing but twine
//	               runs on the loop threads)
//	-mode direct   the TUN write is done on the loop with the frame still in
//	               the shared read buffer (no copy); only Kafka goes through
//	               the goroutine. A non-blocking TUN write never blocks: the
//	               kernel drops the packet if the queue is full.
//
// Kafka is disabled when -brokers is empty.
//
// With -aes every frame body is nonce(12) || AES-256-GCM ciphertext || tag(16)
// and is decrypted on the loop before the TUN write and the sink; the TUN and
// Kafka receive the plaintext. Frames that fail to decrypt are counted in
// decrypt_err and skipped.
//
//	sudo ip tuntap add dev tun0 mode tun user $USER && sudo ip link set tun0 up
//	go run . -tun tun0 -addr :9000 -brokers localhost:9092 -topic tun-packets
package main

import (
	"context"
	"crypto/cipher"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/robert-malhotra/twine"
	"github.com/robert-malhotra/twine/examples/tunsink/internal/tunpkt"
	"github.com/robert-malhotra/twine/proto"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sys/unix"
)

const maxPacket = 65535

type stats struct {
	frames, bytes, tunErr, kafkaOK, kafkaErr, pauses, decryptErr atomic.Uint64
}

func main() {
	iface := flag.String("tun", "tun0", "TUN interface name (empty: discard packets)")
	addr := flag.String("addr", ":9000", "TCP listen address")
	brokers := flag.String("brokers", "", "comma-separated Kafka brokers (empty: Kafka disabled)")
	topic := flag.String("topic", "tun-packets", "Kafka topic")
	kafkaDrop := flag.Bool("kafka-drop", false, "drop records when the Kafka client buffer is full instead of blocking (TryProduce vs Produce)")
	loops := flag.Int("loops", 1, "event loops")
	pin := flag.String("pin", "", "comma-separated CPUs to pin loops to")
	mode := flag.String("mode", "channel", "sink mode: channel or direct")
	sinks := flag.Int("sinks", 1, "sink goroutines draining frames (TUN write in channel mode, Kafka produce)")
	watchdog := flag.Duration("watchdog", 10*time.Millisecond, "slow-callback watchdog threshold (0: off)")
	statsEvery := flag.Duration("stats", time.Second, "stats print interval (0: off)")
	spin := flag.Int("spin", -1, "loop busy-poll iterations before blocking (-1: library default)")
	useAES := flag.Bool("aes", false, "decrypt every frame with AES-256-GCM (nonce || ciphertext || tag)")
	keyHex := flag.String("key", tunpkt.DefaultKeyHex(), "AES-256 key, 64 hex characters")
	flag.Parse()

	var gcm cipher.AEAD
	if *useAES {
		key, err := tunpkt.ParseKey(*keyHex)
		if err != nil {
			log.Fatal(err)
		}
		if gcm, err = tunpkt.NewAEAD(key); err != nil {
			log.Fatal(err)
		}
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
		defer unix.Close(fd)
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

	var st stats
	direct := *mode == "direct"
	if !direct && *mode != "channel" {
		log.Fatalf("unknown -mode %q", *mode)
	}

	// Frames leave the event loops through this channel. One goroutine drains
	// it so blocking work (TUN write in channel mode, the Kafka client) never
	// runs on a loop.
	sink := make(chan []byte, 4096)
	var sinkWG, drainWG sync.WaitGroup
	var stopped atomic.Bool
	var errOnce sync.Once
	promise := func(_ *kgo.Record, err error) {
		if err != nil {
			st.kafkaErr.Add(1)
			errOnce.Do(func() { log.Printf("kafka: first produce error: %v", err) })
		} else {
			st.kafkaOK.Add(1)
		}
	}
	for i := 0; i < *sinks; i++ {
		sinkWG.Add(1)
		go func() {
			defer sinkWG.Done()
			for pkt := range sink {
				if !direct && tunFD >= 0 {
					if _, err := unix.Write(tunFD, pkt); err != nil {
						st.tunErr.Add(1)
					}
				}
				if kc != nil {
					// Produce blocks when the client buffer is full, so Kafka
					// backpressure propagates to the TCP senders. With
					// -kafka-drop, TryProduce drops instead and the promise
					// reports the error.
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

	opts := []twine.Option{twine.Loops(*loops), twine.Watchdog(*watchdog)}
	if *pin != "" {
		var cpus []int
		for _, s := range strings.Split(*pin, ",") {
			var c int
			if _, err := fmt.Sscan(s, &c); err != nil {
				log.Fatalf("-pin: %v", err)
			}
			cpus = append(cpus, c)
		}
		opts = append(opts, twine.PinLoops(cpus...))
	}
	if *spin >= 0 {
		opts = append(opts, twine.SpinIterations(*spin))
	}
	eng := twine.New(opts...)
	ln, err := eng.ListenTCP(*addr, twine.FactoryFunc(func(peer net.Addr) twine.Protocol {
		c := &client{st: &st, sink: sink, direct: direct, tunFD: tunFD, needSink: needSink, gcm: gcm, drains: &drainWG, stopped: &stopped}
		c.lp.MaxFrame = maxPacket
		if gcm != nil {
			c.lp.MaxFrame = maxPacket + tunpkt.AESOverhead
			if !needSink {
				c.scratch = make([]byte, 0, maxPacket)
			}
		}
		c.lp.FrameReceived = c.frame
		c.lp.OnConnectionLost = func(err error) {
			if err != nil && !errors.Is(err, twine.ErrClosed) && !errors.Is(err, twine.ErrShutdown) {
				log.Printf("%s: %v", peer, err)
			}
		}
		return &c.lp
	}))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("tunsink: tcp%s -> tun=%q kafka=%q mode=%s loops=%d aes=%v", ln.Addr(), *iface, *brokers, *mode, *loops, *useAES)

	if *statsEvery > 0 {
		go printStats(ctx, &st, *statsEvery)
	}
	if err := eng.Run(ctx); err != nil {
		log.Fatal(err)
	}
	// The loops are gone, so no new drains start; let the running ones push
	// their backlog into the sink before it is closed.
	stopped.Store(true)
	drainWG.Wait()
	close(sink)
	sinkWG.Wait()
	if kc != nil {
		fctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := kc.Flush(fctx); err != nil {
			log.Printf("kafka flush: %v", err)
		}
		cancel()
	}
	log.Printf("total frames=%d bytes=%d tun_err=%d kafka_ok=%d kafka_err=%d pauses=%d decrypt_err=%d",
		st.frames.Load(), st.bytes.Load(), st.tunErr.Load(), st.kafkaOK.Load(), st.kafkaErr.Load(), st.pauses.Load(), st.decryptErr.Load())
}

// client is one TCP connection. All fields except pending are touched only
// on the connection's loop thread.
type client struct {
	lp       proto.LengthPrefixed
	st       *stats
	sink     chan<- []byte
	direct   bool
	tunFD    int
	needSink bool
	// gcm is non-nil with -aes. scratch is the decrypt buffer (cap maxPacket)
	// used in direct mode with Kafka off, where nothing leaves the loop.
	gcm     cipher.AEAD
	scratch []byte

	// Overflow state. paused is loop-thread only. pending is shared with the
	// drainer goroutine under mu.
	paused  bool
	mu      sync.Mutex
	pending [][]byte
	// drains counts live drain goroutines so shutdown can wait for them before
	// closing the sink; stopped is set once the engine has stopped.
	drains  *sync.WaitGroup
	stopped *atomic.Bool
}

// startDrain launches a drain goroutine (loop thread only).
func (c *client) startDrain() {
	c.drains.Add(1)
	go c.drain()
}

// frame runs on the loop for every complete frame.
func (c *client) frame(f []byte) {
	c.st.frames.Add(1)
	c.st.bytes.Add(uint64(len(f)))
	var pkt []byte // owned copy for the sink, set when decrypted into one
	if c.gcm != nil {
		// Decrypt into the buffer the frame would be copied to anyway: a fresh
		// slice when it goes to the sink, else the reusable scratch buffer.
		dst := c.scratch
		if c.needSink && len(f) >= tunpkt.AESOverhead {
			dst = make([]byte, 0, len(f)-tunpkt.AESOverhead)
		}
		p, ok := tunpkt.Open(c.gcm, dst, f)
		if !ok {
			c.st.decryptErr.Add(1)
			return
		}
		f, pkt = p, p
	}
	if c.direct && c.tunFD >= 0 {
		// Non-blocking TUN fd: the kernel drops rather than blocks.
		if _, err := unix.Write(c.tunFD, f); err != nil {
			c.st.tunErr.Add(1)
		}
	}
	if !c.needSink {
		return
	}
	if c.gcm == nil {
		pkt = append([]byte(nil), f...) // f is only valid during the call
	}
	if !c.paused {
		select {
		case c.sink <- pkt:
			return
		default:
		}
		// Sink is full. Stop reading from this client; one drainer goroutine
		// pushes the backlog and asks the loop to resume. TCP backpressure
		// reaches the sender instead of frames being dropped.
		c.paused = true
		c.st.pauses.Add(1)
		c.lp.Transport().PauseReading()
		c.startDrain()
	}
	// Frames still arrive from the chunk already in the read buffer while
	// paused; queue them for the drainer.
	c.mu.Lock()
	c.pending = append(c.pending, pkt)
	c.mu.Unlock()
}

// drain runs off-loop. It blocks on the sink until pending is empty, then
// hands control back to the loop, which either resumes reading or, if the
// trailing chunk added more frames, drains again.
func (c *client) drain() {
	defer c.drains.Done()
	c.pushPending()
	t := c.lp.Transport()
	t.Loop().CallFromThread(func() {
		// On the loop: no concurrent appends are possible here.
		c.mu.Lock()
		n := len(c.pending)
		c.mu.Unlock()
		if n > 0 {
			c.startDrain()
			return
		}
		c.paused = false
		t.ResumeReading()
	})
	if c.stopped.Load() {
		// The loop may have dropped the callback: nothing appends any more, so
		// flush what the shutdown linger read after the first pass.
		c.pushPending()
	}
}

// pushPending blocks on the sink until pending is empty.
func (c *client) pushPending() {
	for {
		c.mu.Lock()
		batch := c.pending
		c.pending = nil
		c.mu.Unlock()
		if len(batch) == 0 {
			return
		}
		for _, p := range batch {
			c.sink <- p
		}
	}
}

func printStats(ctx context.Context, st *stats, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	var lastF, lastB, lastK uint64
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			f, b, k := st.frames.Load(), st.bytes.Load(), st.kafkaOK.Load()
			dt := now.Sub(last).Seconds()
			fmt.Fprintf(os.Stderr, "stats frames/s=%.0f MB/s=%.1f kafka/s=%.0f pauses=%d tun_err=%d kafka_err=%d decrypt_err=%d\n",
				float64(f-lastF)/dt, float64(b-lastB)/dt/1e6, float64(k-lastK)/dt,
				st.pauses.Load(), st.tunErr.Load(), st.kafkaErr.Load(), st.decryptErr.Load())
			lastF, lastB, lastK, last = f, b, k, now
		}
	}
}
