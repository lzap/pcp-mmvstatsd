package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/performancecopilot/speed"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

const (
	MAX_UNPROCESSED_PACKETS = 2048
	TCP_READ_SIZE           = 4096
)

var signalchan chan os.Signal

type Packet struct {
	Bucket   string
	ValFlt   float64
	ValStr   string
	Modifier string
	Sampling float32
}

func sanitizeBucket(bucket string) string {
	b := make([]byte, len(bucket))
	var bl int

	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		case (c >= byte('a') && c <= byte('z')) ||
			(c >= byte('A') && c <= byte('Z')) ||
			(c >= byte('0') && c <= byte('9')) ||
			c == byte('-') || c == byte('.') ||
			c == byte('_'):
			b[bl] = c
			bl++
		case c == byte(' '):
			b[bl] = byte('_')
			bl++
		case c == byte('/'):
			b[bl] = byte('_')
			bl++
		}
	}
	return string(b[:bl])
}

var (
	serviceAddress    = flag.String("address", ":8125", "UDP service address")
	tcpServiceAddress = flag.String("tcpaddr", "", "TCP service address, if set")
	maxUdpPacketSize  = flag.Int64("max-udp-packet-size", 1472, "Maximum UDP packet size")
	debug             = flag.Bool("debug", false, "print statistics sent to graphite")
	showVersion       = flag.Bool("version", false, "print version string")
	prefix            = flag.String("prefix", "", "Prefix for all stats")
	postfix           = flag.String("postfix", "", "Postfix for all stats")
)

var In = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
var metricMap = make(map[string]speed.Metric)
var histograms = make(map[string]speed.Histogram)
var logging = log.New(os.Stdout, "", 0)

func consume() {
	client, err := speed.NewPCPClient("statsd")
	if err != nil {
		panic(err)
	}
	// keep metrics across restarts
	err = client.SetFlag(0)
	if err != nil {
		panic(err)
	}
	client.MustStart()
	defer client.Stop()
	for {
		select {
		case sig := <-signalchan:
			speed.EraseFileOnStop = true
			logging.Printf("Shutting down on signal %v\n", sig)
			return
		case s := <-In:
			packetHandler(s, client)
		}
	}
}

func findHistogram(client speed.Client, bucket string) (speed.Histogram, error) {
	if histograms[bucket] == nil {
		client.MustStop()
		defer client.MustStart()
		var h, err = speed.NewPCPHistogram(bucket, 0, 86400000, 3, speed.MillisecondUnit) // 0 to 24 hours
		client.MustRegister(h)
		if err != nil {
			logging.Printf("Unable to register histogram %s (%s)\n", bucket, err)
			return nil, fmt.Errorf("Unable to register histogram %s", bucket)
		}
		histograms[bucket] = h
	}
	return histograms[bucket], nil
}

func findMetric(client speed.Client, bucket string, val interface{}, t speed.MetricType, s speed.MetricSemantics, u speed.MetricUnit) (speed.Metric, error) {
	if metricMap[bucket] == nil {
		client.MustStop()
		defer client.MustStart()
		var err error
		metricMap[bucket], err = client.RegisterString(bucket, val, t, s, u)
		if err != nil {
			logging.Printf("Unable to register metric %s (%s)\n", bucket, err)
			return nil, fmt.Errorf("Unable to register metric %s", bucket)
		}
	}
	return metricMap[bucket], nil
}

func packetHandler(s *Packet, client speed.Client) {
	if *debug {
		logging.Printf("Packet: %+v\n", s)
	}

	switch s.Modifier {
	case "ms":
		m, err := findHistogram(client, s.Bucket)
		if err == nil {
			m.MustRecord(int64(s.ValFlt))
			if *debug {
				logging.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
			}
		}
	case "g":
		m, err := findMetric(client, s.Bucket, float64(0), speed.DoubleType, speed.InstantSemantics, speed.OneUnit)
		if err == nil {
			if s.ValStr == "" {
				m.(speed.SingletonMetric).MustSet(s.ValFlt)
				if *debug {
					logging.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
				}
			} else if s.ValStr == "+" {
				m.(speed.SingletonMetric).MustSet(m.(speed.SingletonMetric).Val().(float64) + s.ValFlt)
				if *debug {
					logging.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
				}
			} else if s.ValStr == "-" {
				m.(speed.SingletonMetric).MustSet(m.(speed.SingletonMetric).Val().(float64) - s.ValFlt)
				if *debug {
					logging.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
				}
			}
		}
	case "c":
		m, err := findMetric(client, s.Bucket, int64(0), speed.Int64Type, speed.CounterSemantics, speed.OneUnit)
		if err == nil {
			m.(speed.SingletonMetric).MustSet(m.(speed.SingletonMetric).Val().(int64) + int64(s.ValFlt))
			if *debug {
				logging.Printf("%s %d\n", s.Bucket, m.(speed.SingletonMetric).Val())
			}
		}
	}
}

type MsgParser struct {
	reader       io.Reader
	buffer       []byte
	partialReads bool
	done         bool
}

func NewParser(reader io.Reader, partialReads bool) *MsgParser {
	return &MsgParser{reader, []byte{}, partialReads, false}
}

func (mp *MsgParser) Next() (*Packet, bool) {
	buf := mp.buffer

	for {
		line, rest := mp.lineFrom(buf)

		if line != nil {
			mp.buffer = rest
			return parseLine(line), true
		}

		if mp.done {
			return parseLine(rest), false
		}

		idx := len(buf)
		end := idx
		if mp.partialReads {
			end += TCP_READ_SIZE
		} else {
			end += int(*maxUdpPacketSize)
		}
		if cap(buf) >= end {
			buf = buf[:end]
		} else {
			tmp := buf
			buf = make([]byte, end)
			copy(buf, tmp)
		}

		n, err := mp.reader.Read(buf[idx:])
		buf = buf[:idx+n]
		if err != nil {
			if err != io.EOF {
				logging.Printf("ERROR: %s", err)
			}

			mp.done = true

			line, rest = mp.lineFrom(buf)
			if line != nil {
				mp.buffer = rest
				return parseLine(line), len(rest) > 0
			}

			if len(rest) > 0 {
				return parseLine(rest), false
			}

			return nil, false
		}
	}
}

func (mp *MsgParser) lineFrom(input []byte) ([]byte, []byte) {
	split := bytes.SplitAfterN(input, []byte("\n"), 2)
	if len(split) == 2 {
		return split[0][:len(split[0])-1], split[1]
	}

	if !mp.partialReads {
		if len(input) == 0 {
			input = nil
		}
		return input, []byte{}
	}

	if bytes.HasSuffix(input, []byte("\n")) {
		return input[:len(input)-1], []byte{}
	}

	return nil, input
}

func parseLine(line []byte) *Packet {
	split := bytes.SplitN(line, []byte{'|'}, 3)
	if len(split) < 2 {
		logParseFail(line)
		return nil
	}

	keyval := split[0]
	typeCode := string(split[1])

	sampling := float32(1)
	if strings.HasPrefix(typeCode, "c") || strings.HasPrefix(typeCode, "ms") {
		if len(split) == 3 && len(split[2]) > 0 && split[2][0] == '@' {
			f64, err := strconv.ParseFloat(string(split[2][1:]), 32)
			if err != nil {
				logging.Printf(
					"ERROR: failed to ParseFloat %s - %s",
					string(split[2][1:]),
					err,
				)
				return nil
			}
			sampling = float32(f64)
		}
	}

	split = bytes.SplitN(keyval, []byte{':'}, 2)
	if len(split) < 2 {
		logParseFail(line)
		return nil
	}
	name := string(split[0])
	val := split[1]
	if len(val) == 0 {
		logParseFail(line)
		return nil
	}

	var (
		err      error
		floatval float64
		strval   string
	)

	switch typeCode {
	case "c":
		floatval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			logging.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "g":
		var s string

		if val[0] == '+' || val[0] == '-' {
			strval = string(val[0])
			s = string(val[1:])
		} else {
			s = string(val)
		}
		floatval, err = strconv.ParseFloat(s, 64)
		if err != nil {
			logging.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "s":
		strval = string(val)
	case "ms":
		floatval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			logging.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	default:
		logging.Printf("ERROR: unrecognized type code %q for metric %s", typeCode, name)
		return nil
	}

	return &Packet{
		Bucket:   sanitizeBucket(*prefix + string(name) + *postfix),
		ValFlt:   floatval,
		ValStr:   strval,
		Modifier: typeCode,
		Sampling: sampling,
	}
}

func logParseFail(line []byte) {
	if *debug {
		logging.Printf("ERROR: failed to parse line: %q\n", string(line))
	}
}

func parseTo(conn io.ReadCloser, partialReads bool, out chan<- *Packet) {
	defer conn.Close()

	parser := NewParser(conn, partialReads)
	for {
		p, more := parser.Next()
		if p != nil {
			out <- p
		}

		if !more {
			break
		}
	}
}

func udpListener() {
	address, _ := net.ResolveUDPAddr("udp", *serviceAddress)
	logging.Printf("listening on %s UDP", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		logging.Fatalf("ERROR: ListenUDP - %s", err)
	}

	parseTo(listener, false, In)
}

func tcpListener() {
	address, _ := net.ResolveTCPAddr("tcp", *tcpServiceAddress)
	logging.Printf("listening on %s TCP", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		logging.Fatalf("ERROR: ListenTCP - %s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			logging.Fatalf("ERROR: AcceptTCP - %s", err)
		}
		go parseTo(conn, true, In)
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)
	signal.Notify(signalchan, syscall.SIGINT)

	go udpListener()
	if *tcpServiceAddress != "" {
		go tcpListener()
	}
	consume()
}
