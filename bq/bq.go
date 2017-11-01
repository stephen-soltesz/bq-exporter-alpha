package bq

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"

	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	runner QueryRunner
	name   string
	query  string

	valType prometheus.ValueType
	desc    *prometheus.Desc

	metrics []Metric
	mux     sync.Mutex
}

// metric holds raw data from query results needed to create a prometheus.Metric.
type Metric struct {
	labels []string
	values []string
	value  float64
}

// NewCollector creates a new BigQuery Collector instance.
func NewCollector(runner QueryRunner, valType prometheus.ValueType, metricName, query string) *Collector {
	return &Collector{
		runner:  runner,
		name:    metricName,
		query:   query,
		valType: valType,
		desc:    nil,
		metrics: nil,
		mux:     sync.Mutex{},
	}
}

// Describe satisfies the prometheus.Collector interface. Describe is called
// immediately after registering the collector.
func (bq *Collector) Describe(ch chan<- *prometheus.Desc) {
	if bq.desc == nil {
		// TODO: collect metrics for query exec time.
		bq.Update()
		bq.setDesc()
	}
	// NOTE: if Update returns no metrics, this will fail.
	ch <- bq.desc
}

// Collect satisfies the prometheus.Collector interface. Collect reports values
// from cached metrics.
func (bq *Collector) Collect(ch chan<- prometheus.Metric) {
	bq.mux.Lock()
	defer bq.mux.Unlock()

	for i := range bq.metrics {
		ch <- prometheus.MustNewConstMetric(
			bq.desc, bq.valType, bq.metrics[i].value, bq.metrics[i].values...)
	}
}

// String satisfies the Stringer interface. String returns the metric name.
func (bq *Collector) String() string {
	return bq.name
}

// Update runs the collector query and atomically updates the cached metrics.
// Update is called automaticlly after the collector is registered.
func (bq *Collector) Update() error {
	metrics, err := bq.runner.Query(bq.query)
	if err != nil {
		return err
	}
	// Swap the cached metrics.
	bq.mux.Lock()
	defer bq.mux.Unlock()
	bq.metrics = metrics
	return nil
}

func (bq *Collector) setDesc() {
	// The query may return no results.
	if len(bq.metrics) > 0 {
		// TODO: allow passing meaningful help text.
		bq.desc = prometheus.NewDesc(bq.name, "help text", bq.metrics[0].labels, nil)
	} else {
		// TODO: this is a problem.
		return
	}
}

// rowToMetric converts a bigquery result row to a bq.Metric
func rowToMetric(row map[string]bigquery.Value) Metric {
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
