package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/peterbourgon/ff/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"sgrankin.dev/netatmo-otel/netatmo"

	promclient "github.com/prometheus/client_golang/api"
	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

var (
	_ = flag.String("config", "", "config file (optional)")

	accessToken  = flag.String("access-token", "", "Oauth2 Access Token")
	refreshToken = flag.String("refresh-token", "", "Oauth2 Refresh Token")
	clientID     = flag.String("client-id", "", "Oauth2 Client ID")
	clientSecret = flag.String("client-secret", "", "Oauth2 Client Secret")

	dest = flag.String("dest", "",
		"Destination host:port. Must accept Prometheus queries and OTLP pushes at routes matching VictoriaMetrics.")

	resume = flag.String("resume", "",
		"The resume token that was logged.  Will skip as many requests as possible to avoid duplicate work..")

	incremental = flag.Bool("incremental", true,
		"Query for the last timestamp exported and start scrape from there.")
	incrementalSince = flag.Duration("incremental-since", 30*24*time.Hour,
		"Query this far back to find the last written sample. If not found, uses -since as the starting point.")
	scrapeSince = flag.Duration("since", 0,
		"Start scrape this long ago. Set 0 to disable and start from the first recorded sample in netatmo.")

	verbose = flag.Bool("verbose", false, "Verbose logging")
)

func init() {
	err := ff.Parse(flag.CommandLine, os.Args[1:],
		ff.WithEnvVars(),
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	)
	switch {
	case err == nil:
	case errors.Is(err, flag.ErrHelp):
		flag.Usage()
		os.Exit(2)
	default:
		log.Fatal(err)
	}
}

func main() {
	ctx := context.Background()
	var exporter exporter
	if *dest != "" {
		exporter, _ = otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
			otlpmetrichttp.WithHeaders(map[string]string{"Content-Encoding": "gzip"}),
			otlpmetrichttp.WithEndpoint(*dest),
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithURLPath("/opentelemetry/api/v1/push"),
		)
	} else {
		exporter, _ = stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	}
	client := netatmo.NewClient(ctx, *clientID, *clientSecret, *accessToken, *refreshToken)

	promClient, err := promclient.NewClient(promclient.Config{Address: "http://" + *dest})
	if err != nil {
		log.Fatal(err)
	}
	promAPI := promapi.NewAPI(promClient)

	stations, err := client.GetStations(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, dev := range stations {
		if *verbose {
			log.Printf("exporting device %q", dev.ID)
		}
		commonAttrs := []attribute.KeyValue{
			attribute.String("home_id", dev.HomeID),
			attribute.String("home_name", dev.HomeName),
		}
		attrs := attribute.NewSet(append(commonAttrs,
			attribute.String("dev_id", string(dev.ID)),
			attribute.String("module_name", dev.Name),
			attribute.String("module_type", string(dev.Type)),
			// attribute.Int("firmware", dev.Firmware),
		)...)
		exportHistory(ctx, client, promAPI, exporter, attrs, dev.ID, "", dev.DataTypes)

		for _, mod := range dev.Modules {
			if *verbose {
				log.Printf("exporting device %q module %q", dev.ID, mod.ID)
			}
			attrs := attribute.NewSet(append(commonAttrs,
				attribute.String("dev_id", string(mod.ID)),
				attribute.String("module_name", mod.Name),
				attribute.String("module_type", string(mod.Type)),
				// attribute.Int("firmware", dev.Firmware),
			)...)
			exportHistory(ctx, client, promAPI, exporter, attrs, dev.ID, mod.ID, mod.DataTypes)
		}
	}
}

func exportHistory(
	ctx context.Context,
	client *netatmo.Client, promAPI promapi.API,
	exporter exporter, attrs attribute.Set,
	device netatmo.DeviceID, module netatmo.ModuleID,
	dataTypes []netatmo.DataType,
) {
	var since time.Time
	if *incremental {
		val, _, err := promAPI.Query(ctx,
			fmt.Sprintf("timestamp(netatmo_%s[%s])", strings.ToLower(string(dataTypes[0])), incrementalSince.String()),
			time.Now())
		if err != nil {
			log.Fatal(err)
		}
		vec := val.(model.Vector)
		for _, sample := range vec {
			if module != "" && string(sample.Metric["dev_id"]) == string(module) || module == "" && string(sample.Metric["dev_id"]) == string(device) {
				since = time.Unix(int64(sample.Value), 0).Add(time.Second)
				break
			}
		}
	}
	if since.IsZero() && *scrapeSince != 0 {
		since = time.Now().Add(-*scrapeSince)
	}

	// Resume token present?
	if *resume != "" {
		r := strings.Split(*resume, "/")
		if r[0] != string(device) || r[1] != string(module) {
			// Token was given and it has some other module.. probably skip ahead.
			return
		}
		sec, err := strconv.Atoi(r[2])
		if err != nil {
			log.Fatal(err)
		}
		since = time.Unix(int64(sec), 0)
		*resume = ""
	}

	err := client.GetMeasure(ctx, device, module, dataTypes, since, func(points []netatmo.DataPoint, nextTime time.Time) error {
		// Gauges contain the datapoints.
		gauges := make([]metricdata.Gauge[float64], len(dataTypes))
		for _, point := range points {
			for i := range dataTypes {
				gauges[i].DataPoints = append(gauges[i].DataPoints, metricdata.DataPoint[float64]{
					Attributes: attrs,
					Time:       point.Time,
					Value:      point.Values[i],
				},
				)
			}
		}
		// Metrics give the gauges a name and units.
		metrics := []metricdata.Metrics{}
		for i, gauge := range gauges {
			dt := dataTypes[i]
			metrics = append(metrics, metricdata.Metrics{
				Name: "netatmo_" + strings.ToLower(string(dt)),
				Data: gauge,
				Unit: netatmo.DataUnits[dt],
			},
			)
		}

		rm := &metricdata.ResourceMetrics{ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: metrics}}}

		if *verbose {
			log.Printf("Exporting %d datapoints", len(gauges[0].DataPoints))
		}
		if err := exporter.Export(ctx, rm); err != nil {
			return err
		}
		if *verbose {
			log.Printf("Resume token: %s/%s/%d", device, module, nextTime.Unix())
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

type exporter interface {
	Export(ctx context.Context, rm *metricdata.ResourceMetrics) error
}
