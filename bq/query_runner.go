package bq

import (
	"context"
	"log"

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

// TODO: evaluate query as a template?
func (qr *queryRunnerImpl) Query(query string) ([]Metric, error) {
	metrics := []Metric{}

	q := qr.client.Query(query)

	// TODO: check query string for SQL type.
	//	q.QueryConfig.UseLegacySQL = true

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
