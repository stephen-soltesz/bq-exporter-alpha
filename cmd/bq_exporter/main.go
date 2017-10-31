package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	flag "github.com/spf13/pflag"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	querySources = []string{}
	project      = flag.String("project", "", "GCP project name.")
	refresh      = flag.Duration("refresh", 15*time.Minute, "Number of seconds between refreshing.")
)

// TODO: can we use prometheus.Metric types?
type Metric struct {
	labels []string
	values []string
	value  float64
}

func init() {
	// Register flags.
	flag.StringArrayVar(&querySources, "query", nil, "Name of file with query string.")
}

type bqCollector struct {
	client *bigquery.Client
	name   string
	query  string

	valType prometheus.ValueType
	desc    *prometheus.Desc

	metrics []Metric
	mux     sync.Mutex
}

func NewBQCollector(client *bigquery.Client, valType prometheus.ValueType, metricName, query string) *bqCollector {
	query = strings.Replace(query, "UNIX_START_TIME", fmt.Sprintf("%d", time.Now().UTC().Unix()), -1)
	fmt.Println(query)
	return &bqCollector{
		client,
		metricName,
		query,
		valType,
		nil,
		nil,
		sync.Mutex{},
	}
}

func (bq *bqCollector) Describe(ch chan<- *prometheus.Desc) {
	fmt.Println("Describe")
	ch <- bq.desc
}

func (bq *bqCollector) Collect(ch chan<- prometheus.Metric) {
	fmt.Println("Collect")
	bq.mux.Lock()
	defer bq.mux.Unlock()
	for j := range bq.metrics {
		log.Printf("Updating gauge for: %#v", bq.metrics[j])
		ch <- prometheus.MustNewConstMetric(
			bq.desc, prometheus.GaugeValue, bq.metrics[j].value, bq.metrics[j].values...)
	}
}

func (bq *bqCollector) runQuery() {
	metrics := []Metric{}

	q := bq.client.Query(bq.query)
	// TODO: check query string for sql type.
	q.QueryConfig.UseLegacySQL = true
	// TODO: evaluate query as a template.

	it, err := q.Read(context.Background())
	if err != nil {
		// TODO: Handle error.
		log.Fatal(err)
	}

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
			log.Fatal(err)
		}

		metrics = append(metrics, toMetric(row))
		// fmt.Printf("%#v\n", row)
	}
	if bq.desc == nil {
		if len(metrics) > 0 {
			bq.desc = prometheus.NewDesc(
				bq.name,
				"help text",
				metrics[0].labels,
				nil)
		} else {
			return
		}
	}
	bq.mux.Lock()
	defer bq.mux.Unlock()
	bq.metrics = metrics
}

func toMetric(row map[string]bigquery.Value) Metric {
	m := Metric{}
	// Since `range` does not guarantee map key order, we must extract, sort
	// and then extract values.
	for k, v := range row {
		if strings.HasPrefix(k, "label_") {
			m.labels = append(m.labels, strings.TrimPrefix(k, "label_"))
		}
		if k == "value" {
			// TODO: type cast is fragile. check for int or float or error.
			m.value = (float64)(v.(int64))
		}
	}
	sort.Strings(m.labels)

	for i := range m.labels {
		// TODO: check type assertion.
		key := fmt.Sprintf("label_%s", m.labels[i])
		m.values = append(m.values, row[key].(string))
	}
	return m
}

func createGauge(name, help string, labels []string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: help,
		},
		labels,
	)
	prometheus.MustRegister(g)
	return g
}

func sleepUntilNext(d time.Duration) {
	// Truncate the current time to a multiple of interval. Then add the
	// interval to move the time into the future.
	next := time.Now().Truncate(d).Add(d)
	// Wait until we are aligned on the next interval.
	time.Sleep(time.Until(next))
}

func filenameToMetric(filename string) string {
	fname := filepath.Base(filename)
	return strings.TrimSuffix(fname, filepath.Ext(fname))
}

func createCollector(typeName, filename string) *bqCollector {
	query, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *project)
	if err != nil {
		log.Fatal(err)
	}

	var v prometheus.ValueType
	if typeName == "collector" {
		v = prometheus.CounterValue
	} else if typeName == "gauge" {
		v = prometheus.GaugeValue
	} else {
		v = prometheus.UntypedValue
	}

	return NewBQCollector(client, v, filenameToMetric(filename), string(query))
}

func main() {
	flag.Parse()
	var bqGauges = []*prometheus.GaugeVec{}
	var metricNames = []string{}
	var queryFiles = []string{}
	var collectors = []*bqCollector{}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":9393", nil))
	}()

	// TODO: we must create a guauge after runnign the query once to extract
	// the labels.
	for i := range querySources {
		keyVal := strings.SplitN(querySources[i], "=", 2)
		metricNames = append(metricNames, keyVal[0])
		queryFiles = append(queryFiles, keyVal[1])
		collectors = append(collectors, createCollector(keyVal[0], keyVal[1]))
		bqGauges = append(bqGauges, nil)
	}

	for i := range collectors {
		collectors[i].runQuery()
		prometheus.MustRegister(collectors[i])
	}

	for sleepUntilNext(*refresh); ; sleepUntilNext(*refresh) {
		log.Printf("Starting a new round at: %s", time.Now())

		for i := range queryFiles {
			log.Printf("Running query for %s", queryFiles[i])
			collectors[i].runQuery()
		}
	}
}
