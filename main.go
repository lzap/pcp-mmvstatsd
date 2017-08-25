package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/performancecopilot/speed"
	"io"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

const (
	MAX_UNPROCESSED_PACKETS = 1000
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

type Float64Slice []float64

func (s Float64Slice) Len() int           { return len(s) }
func (s Float64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Float64Slice) Less(i, j int) bool { return s[i] < s[j] }

type Percentiles []*Percentile
type Percentile struct {
	float float64
	str   string
}

func (a *Percentiles) Set(s string) error {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*a = append(*a, &Percentile{f, strings.Replace(s, ".", "_", -1)})
	return nil
}
func (p *Percentile) String() string {
	return p.str
}
func (a *Percentiles) String() string {
	return fmt.Sprintf("%v", *a)
}

func sanitizeBucket(bucket string) string {
	b := make([]byte, len(bucket))
	var bl int

	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		case (c >= byte('a') && c <= byte('z')) || (c >= byte('A') && c <= byte('Z')) || (c >= byte('0') && c <= byte('9')) || c == byte('-') || c == byte('.') || c == byte('_'):
			b[bl] = c
			bl++
		case c == byte(' '):
			b[bl] = byte('_')
			bl++
		case c == byte('/'):
			b[bl] = byte('-')
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
var knownBuckets = make(map[string]speed.Metric)
var log = logrus.New()

func initLogging() {
	//log.Formatter = new(prefixed.TextFormatter)
	log.Level = logrus.DebugLevel
}

func consume() {
	if *debug {
		speed.EnableLogging(true)
	}
	client, err := speed.NewPCPClient("statsd")
	if err != nil {
		panic(err)
	}
	client.MustStart()
	defer client.Stop()
	for {
		select {
		case sig := <-signalchan:
			speed.EraseFileOnStop = true
			log.Printf("Shutting down on signal %v\n", sig)
			return
		case s := <-In:
			packetHandler(s, client)
		}
	}
}

func findBucket(client speed.Client, bucket string, val interface{}, t speed.MetricType, s speed.MetricSemantics, u speed.MetricUnit) speed.Metric {
	if knownBuckets[bucket] == nil {
		client.MustStop()
		knownBuckets[bucket] = client.MustRegisterString(bucket, val, t, s, u)
		client.MustStart()
	}
	return knownBuckets[bucket]
}

func packetHandler(s *Packet, client speed.Client) {
	if *debug {
		log.Printf("DEBUG: rcvd packet: %+v\n", s)
	}

	switch s.Modifier {
	case "ms":
		findBucket(client, s.Bucket, float64(0), speed.DoubleType, speed.DiscreteSemantics, speed.MillisecondUnit).(speed.SingletonMetric).MustSet(s.ValFlt)
		log.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
	case "g":
		m := findBucket(client, s.Bucket, float64(0), speed.DoubleType, speed.InstantSemantics, speed.OneUnit).(speed.SingletonMetric)
		if s.ValStr == "" {
			m.MustSet(s.ValFlt)
			log.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
		} else if s.ValStr == "+" {
			m.MustSet(m.Val().(float64) + s.ValFlt)
			log.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
		} else if s.ValStr == "-" {
			m.MustSet(m.Val().(float64) - s.ValFlt)
			log.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
		}
	case "c":
		findBucket(client, s.Bucket, int64(0), speed.Int64Type, speed.CounterSemantics, speed.OneUnit).(speed.SingletonMetric).MustSet(int64(s.ValFlt))
		log.Printf("%s %s\n", s.Bucket, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
	case "s":
		// NOT IMPLEMENTED
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
				log.Printf("ERROR: %s", err)
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
				log.Printf(
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
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
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
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "s":
		strval = string(val)
	case "ms":
		floatval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	default:
		log.Printf("ERROR: unrecognized type code %q for metric %s", typeCode, name)
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
		log.Printf("ERROR: failed to parse line: %q\n", string(line))
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
	log.Printf("listening on %s UDP", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}

	parseTo(listener, false, In)
}

func tcpListener() {
	address, _ := net.ResolveTCPAddr("tcp", *tcpServiceAddress)
	log.Printf("listening on %s TCP", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenTCP - %s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Fatalf("ERROR: AcceptTCP - %s", err)
		}
		go parseTo(conn, true, In)
	}
}

func main() {
	initLogging()
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
