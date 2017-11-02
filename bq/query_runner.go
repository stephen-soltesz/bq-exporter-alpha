package bq

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type QueryRunner interface {
	Query(q string) ([]Metric, error)
}

type queryRunnerImpl struct {
	client *bigquery.Client
}

// NewQueryRunner creates a new query runner instance.
func NewQueryRunner(client *bigquery.Client) QueryRunner {
	return &queryRunnerImpl{client}
}

// Query executes the given query
func (qr *queryRunnerImpl) Query(query string) ([]Metric, error) {
	metrics := []Metric{}

	q := qr.client.Query(query)

	// TODO: check query string for SQL type.
	q.QueryConfig.UseLegacySQL = true

	// TODO: add context timeout.
	it, err := q.Read(context.Background())
	if err != nil {
		log.Print(err)
		return nil, err
	}

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("%#v %d", err, len(metrics))
			return nil, err
		}
		metrics = append(metrics, rowToMetric(row))
	}
	return metrics, nil
}

// valToFloat extracts a float from the bigquery.Value irrespective of the
// underlying type. If the type is not int64, float64, then valToFloat returns
// zero.
func valToFloat(v bigquery.Value) float64 {
	switch v.(type) {
	case int64:
		return (float64)(v.(int64))
	case float64:
		return v.(float64)
	default:
		return 0
	}
}

// valToString coerces the bigquery.Value into a string. If the underlying type
// is not a string, we treat it like an int64 or float64. If the underlying is
// none of these types, valToString returns "0".
func valToString(v bigquery.Value) string {
	var s string
	switch v.(type) {
	case string:
		s = v.(string)
	default:
		// This should not happen too often, but if it does, hardcode 2 decimal
		// points to try to protect label value cardinailty.
		s = fmt.Sprintf("%.2f", valToFloat(v))
	}
	return s
}

// rowToMetric converts a bigquery result row to a bq.Metric
func rowToMetric(row map[string]bigquery.Value) Metric {
	m := Metric{}
	// Since `range` does not guarantee map key order, we must extract, sort
	// and then extract values.
	for k, v := range row {
		if strings.HasPrefix(k, "label_") {
			m.labels = append(m.labels, strings.TrimPrefix(k, "label_"))
		} else if k == "value" {
			m.value = valToFloat(v)
		}
	}
	sort.Strings(m.labels)

	for i := range m.labels {
		key := fmt.Sprintf("label_%s", m.labels[i])
		m.values = append(m.values, valToString(row[key]))
	}
	return m
}
