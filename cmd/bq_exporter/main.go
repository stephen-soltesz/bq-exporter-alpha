package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/stephen-soltesz/bq-exporter-alpha/bq"

	flag "github.com/spf13/pflag"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	querySources = []string{}
	project      = flag.String("project", "", "GCP project name.")
	refresh      = flag.Duration("refresh", 15*time.Minute, "Number of seconds between refreshing.")
)

func init() {
	// Register flags.
	flag.StringArrayVar(&querySources, "query", nil, "Name of file with query string.")
}

func sleepUntilNext(d time.Duration) {
	// Truncate the current time to a multiple of interval. Then add the
	// interval to move the time into the future.
	next := time.Now().Truncate(d).Add(d)
	// Wait until we are aligned on the next interval.
	time.Sleep(time.Until(next))
}

func fileToMetric(filename string) string {
	fname := filepath.Base(filename)
	return strings.TrimSuffix(fname, filepath.Ext(fname))
}

func registerCollector(typeName, filename string, refresh time.Duration) *bq.Collector {
	queryBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *project)
	if err != nil {
		log.Fatal(err)
	}

	var v prometheus.ValueType
	if typeName == "counter" {
		v = prometheus.CounterValue
	} else if typeName == "gauge" {
		v = prometheus.GaugeValue
	} else {
		v = prometheus.UntypedValue
	}

	query := string(queryBytes)
	query = strings.Replace(query, "UNIX_START_TIME", fmt.Sprintf("%d", time.Now().UTC().Unix()), -1)
	query = strings.Replace(query, "REFRESH_RATE_SEC", fmt.Sprintf("%d", int(refresh.Seconds())), -1)

	c := bq.NewCollector(bq.NewQueryRunner(client), v, fileToMetric(filename), string(query))
	log.Println("Initializing collector:", c)
	prometheus.MustRegister(c)
	return c
}

func updatePeriodically(collectors []*bq.Collector, refresh time.Duration) {
	for sleepUntilNext(refresh); ; sleepUntilNext(refresh) {
		log.Printf("Starting a new round at: %s", time.Now())
		for i := range collectors {
			log.Printf("Running query for %s", collectors[i])
			collectors[i].Update()
			log.Printf("Done")
		}
	}
}

func main() {
	flag.Parse()
	var collectors = []*bq.Collector{}

	for i := range querySources {
		keyVal := strings.SplitN(querySources[i], "=", 2)
		collectors = append(collectors, registerCollector(keyVal[0], keyVal[1], *refresh))
	}

	go updatePeriodically(collectors, *refresh)

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":9393", nil))
}
