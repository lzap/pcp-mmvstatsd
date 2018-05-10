# Statsd MMV agent for Performance Co-Pilot

Aggregates statsd packets recieved via UDP or TCP and sends them into PCP via
MMV API. Mapping is currently 1:1 into PCP namespace, there is no possibility
to map statsd metrics to instances. The following statsd types are supported:

* example.counter => example.counter
* example.gauge => example.gauge
* example.duration => example.duration[mean,min,max,variance,standard_deviation]

Duration metric is aggregated using HDRHistogram with buckets set from 0 to 24
hours with 3 significant digits (currently hardcoded) and mean, minimum,
maximum, variance and standard deviation is calculated for each PCP update.

## Usage

Install PCP, enable MMV PMDA and run this daemon. Metrics will appear under
mmv.* namespace.

    $ pcp-mmvstatsd -h
    Usage of ./pcp-mmvstatsd:
      -address string
            UDP service address (default ":8125")
      -debug
            print statistics sent to graphite
      -max-udp-packet-size int
            Maximum UDP packet size (default 1472)
      -postfix string
            Postfix for all stats
      -prefix string
            Prefix for all stats
      -tcpaddr string
            TCP service address, if set
      -version
            print version string

## Licensing

See LICENSE for more info. Dependencies are covered under MIT and BSD.

## Authors

* Lukáš Zapletal
* Based on https://github.com/bitly/statsdaemon (bc7b0bcddb5)
