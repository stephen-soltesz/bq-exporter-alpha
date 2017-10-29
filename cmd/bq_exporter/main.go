package main

import (
	"fmt"
	flag "github.com/spf13/pflag"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

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

func runQuery(file string) []Metric {
	var metrics = []Metric{}
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *project)
	if err != nil {
		// TODO: Handle error.
		log.Println(err)
		return metrics
	}

	// TODO: read file once.
	b, err := ioutil.ReadFile(file)
	if err != nil {
		log.Println(err)
		return metrics
	}

	q := client.Query((string)(b))
	// TODO: check query string for sql type.
	q.QueryConfig.UseLegacySQL = true
	// TODO: evaluate query as a template.

	it, err := q.Read(ctx)
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
		fmt.Printf("%#v\n", row)
	}

	return metrics
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

func main() {
	flag.Parse()
	var bqGauges = []*prometheus.GaugeVec{}
	var metricNames = []string{}
	var queryFiles = []string{}
	var start time.Time

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
		bqGauges = append(bqGauges, nil)
	}

	for ; ; time.Sleep(*refresh - time.Since(start)) {
		start = time.Now()
		log.Printf("Starting a new round at: %s", start)

		for i := range queryFiles {
			log.Printf("Running query for %s", queryFiles[i])
			metrics := runQuery(queryFiles[i])
			if len(metrics) == 0 {
				log.Printf("Got no metrics")
				continue
			}
			if bqGauges[i] == nil {
				log.Printf("Creating gauge for: %#v", metrics[0])
				bqGauges[i] = createGauge(metricNames[i], queryFiles[i], metrics[0].labels)
			}
			for j := range metrics {
				log.Printf("Updating gauge for: %#v", metrics[j])
				bqGauges[i].WithLabelValues(metrics[j].values...).Set(metrics[j].value)
			}
		}
	}
}
