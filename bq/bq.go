package bq

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

// TODO: can we use prometheus.Metric types?
type Metric struct {
	labels []string
	values []string
	value  float64
}

type Collector struct {
	client *bigquery.Client
	name   string
	query  string

	valType prometheus.ValueType
	desc    *prometheus.Desc

	metrics []Metric
	mux     sync.Mutex
}

func NewCollector(client *bigquery.Client, valType prometheus.ValueType, metricName, query string) *Collector {
	fmt.Println(query)
	return &Collector{
		client,
		metricName,
		query,
		valType,
		nil,
		nil,
		sync.Mutex{},
	}
}

func (bq *Collector) Describe(ch chan<- *prometheus.Desc) {
	fmt.Println("Describe")
	ch <- bq.desc
}

func (bq *Collector) Collect(ch chan<- prometheus.Metric) {
	fmt.Println("Collect")
	bq.mux.Lock()
	defer bq.mux.Unlock()
	for j := range bq.metrics {
		log.Printf("Updating gauge for: %#v", bq.metrics[j])
		ch <- prometheus.MustNewConstMetric(
			bq.desc, prometheus.GaugeValue, bq.metrics[j].value, bq.metrics[j].values...)
	}
}

func (bq *Collector) String() string {
	return bq.name
}

func (bq *Collector) RunQuery() {
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
	}
	if bq.desc == nil {
		// The query may return no results.
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
