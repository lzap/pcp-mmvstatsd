package main

import (
	"bytes"
	"flag"
	"fmt"
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
	MAX_UNPROCESSED_PACKETS = 2048
	TCP_READ_SIZE           = 4096
)

var signalchan chan os.Signal

type Packet struct {
	Metric   string
	ValFlt   float64
	ValStr   string
	Modifier string
	Sampling float32
}

var (
	serviceAddress    = flag.String("address", ":8125", "UDP service address")
	tcpServiceAddress = flag.String("tcpaddr", "", "TCP service address, if set")
	maxUdpPacketSize  = flag.Int64("max-udp-packet-size", 1472, "Maximum UDP packet size")
	debug             = flag.Bool("debug", false, "enable debugging output")
	verbose           = flag.Bool("verbose", false, "enable verbose output")
	trace             = flag.Bool("trace", false, "enable tracing output")
	showVersion       = flag.Bool("version", false, "print version string")
	//mappingFile       = flag.String("mapping", "mapping.json", "Mapping file (JSON format)")
)

var In = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
var metricMap = make(map[string]speed.Metric)
var histograms = make(map[string]speed.Histogram)

func consume() {
	creg := NewClientRegistry()
	defer creg.Stop()
	for {
		select {
		case sig := <-signalchan:
			DebugLog.Printf("Shutting down on signal %v\n", sig)
			return
		case s := <-In:
			packetHandler(s, creg)
		}
	}
}

func findHistogram(creg ClientRegistry, bucket string) (speed.Histogram, error) {
	if histograms[bucket] == nil {
		client, err := creg.FindClientForMetric(bucket)
		if err != nil {
			panic(err)
		}
		client.MustStop()
		defer client.MustStart()
		h, err := speed.NewPCPHistogram(bucket, 0, 86400000, 3, speed.MillisecondUnit) // 0 to 24 hours
		client.MustRegister(h)
		if err != nil {
			ErrorLog.Printf("Unable to register histogram %s (%s)\n", bucket, err)
			return nil, fmt.Errorf("Unable to register histogram %s", bucket)
		}
		histograms[bucket] = h
	}
	return histograms[bucket], nil
}

func findMetric(creg ClientRegistry, name string, val interface{}, t speed.MetricType, s speed.MetricSemantics, u speed.MetricUnit) (speed.Metric, error) {
	metricFound, ok := metricMap[name]
	if ok {
		return metricFound, nil
	} else {
		client, err := creg.FindClientForMetric(name)
		if err != nil {
			panic(err)
		}
		client.MustStop()
		defer client.MustStart()
		metric, err := client.RegisterString(name, val, t, s, u)
		if err != nil {
			ErrorLog.Printf("Unable to register metric %s (%s)\n", name, err)
			return nil, fmt.Errorf("Unable to register metric %s", name)
		}
		metricMap[name] = metric
		return metric, nil
	}
}

func packetHandler(s *Packet, creg ClientRegistry) {
	//TraceLog.Printf("Packet: %+v\n", s)

	switch s.Modifier {
	case "ms":
		m, err := findHistogram(creg, s.Metric)
		if err == nil {
			m.MustRecord(int64(s.ValFlt))
			if *trace {
				TraceLog.Printf("%s %s\n", s.Metric, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
			}
		}
	case "g":
		m, err := findMetric(creg, s.Metric, float64(0), speed.DoubleType, speed.InstantSemantics, speed.OneUnit)
		if err == nil {
			if s.ValStr == "" {
				m.(speed.SingletonMetric).MustSet(s.ValFlt)
				if *trace {
					TraceLog.Printf("%s %s\n", s.Metric, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
				}
			} else if s.ValStr == "+" {
				m.(speed.SingletonMetric).MustSet(m.(speed.SingletonMetric).Val().(float64) + s.ValFlt)
				if *trace {
					TraceLog.Printf("%s %s\n", s.Metric, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
				}
			} else if s.ValStr == "-" {
				m.(speed.SingletonMetric).MustSet(m.(speed.SingletonMetric).Val().(float64) - s.ValFlt)
				if *trace {
					TraceLog.Printf("%s %s\n", s.Metric, strconv.FormatFloat(s.ValFlt, 'f', -1, 64))
				}
			}
		}
	case "c":
		m, err := findMetric(creg, s.Metric, int64(0), speed.Int64Type, speed.CounterSemantics, speed.OneUnit)
		if err == nil {
			m.(speed.SingletonMetric).MustSet(m.(speed.SingletonMetric).Val().(int64) + int64(s.ValFlt))
			if *trace {
				TraceLog.Printf("%s %d\n", s.Metric, m.(speed.SingletonMetric).Val())
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
				ErrorLog.Printf("ERROR: %s", err)
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

func sanitizeName(name string) string {
	b := make([]byte, len(name))
	var bl int

	for i := 0; i < len(name); i++ {
		c := name[i]
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
				ErrorLog.Printf("ERROR: failed to ParseFloat %s - %s", string(split[2][1:]), err)
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
			ErrorLog.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
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
			ErrorLog.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "s":
		strval = string(val)
	case "ms":
		floatval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			ErrorLog.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	default:
		ErrorLog.Printf("ERROR: unrecognized type code %q for metric %s", typeCode, name)
		return nil
	}

	return &Packet{
		Metric:   sanitizeName(name),
		ValFlt:   floatval,
		ValStr:   strval,
		Modifier: typeCode,
		Sampling: sampling,
	}
}

func logParseFail(line []byte) {
	ErrorLog.Printf("ERROR: failed to parse line: %q\n", string(line))
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
	DebugLog.Printf("listening on %s UDP", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		ErrorLog.Fatalf("ListenUDP - %s", err)
	}

	parseTo(listener, false, In)
}

func tcpListener() {
	address, _ := net.ResolveTCPAddr("tcp", *tcpServiceAddress)
	DebugLog.Printf("listening on %s TCP", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		ErrorLog.Fatalf("ListenTCP - %s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			ErrorLog.Fatalf("AcceptTCP - %s", err)
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

	EnableLoggers(*trace, TraceLog, DebugLog, VerboseLog)
	EnableLoggers(*debug, DebugLog, VerboseLog)
	EnableLoggers(*verbose, VerboseLog)
	if *trace {
		*debug = true
		*verbose = true
	}
	if *debug {
		*verbose = true
	}

	/*
	// not currently implemented
	err := NewFromFile(*mappingFile)
	if err != nil {
		ErrorLog.Printf("Unable to read mapping file %s (%s)\n", *mappingFile, err)
		os.Exit(1)
	}
	*/

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)
	signal.Notify(signalchan, syscall.SIGINT)

	go udpListener()
	if *tcpServiceAddress != "" {
		go tcpListener()
	}
	consume()
	os.Exit(0)
}
