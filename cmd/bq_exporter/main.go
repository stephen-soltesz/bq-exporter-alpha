package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	queryFile = flag.String("query", "", "Name of file with query string.")
)

var (
	bqGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bq_ndt_tests",
			Help: "Current number of tests.",
		},
		[]string{"direction", "server"},
	)
)

func init() {
	// Metrics have to be registered to be exposed:
	prometheus.MustRegister(bqGauge)
}

func main() {
	flag.Parse()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":9292", nil))
	}()

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "mlab-sandbox")
	if err != nil {
		// TODO: Handle error.
		log.Fatal(err)
	}

	b, err := ioutil.ReadFile(*queryFile)
	if err != nil {
		log.Fatal(err)
	}

	for {
		q := client.Query((string)(b))
		q.QueryConfig.UseLegacySQL = true
		it, err := q.Read(ctx)
		if err != nil {
			// TODO: Handle error.
			log.Fatal(err)
		}
		type Metric struct {
			name   string
			labels []string
			values []string
			value  float64
		}

		for {
			// var values []bigquery.Value
			var row map[string]bigquery.Value
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				// TODO: Handle error.
				log.Fatal(err)
			}
			bqGauge.WithLabelValues(row["label_direction"].(string), row["label_server"].(string)).Set((float64)(row["value"].(int64)))
			fmt.Printf("%#v\n", row)
		}
		time.Sleep(60 * time.Minute)
	}
}

// bq_ndt_uploads{machine="", day=""}
