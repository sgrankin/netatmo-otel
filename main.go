package main

import (
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"maps"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/peterbourgon/ff/v4"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"tailscale.com/jsondb"

	"sgrankin.dev/netatmo-otel/netatmo"

	promclient "github.com/prometheus/client_golang/api"
	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

var (
	_ = flag.String("config", "", "config file (optional)")

	dest = flag.String("dest", "",
		"Destination host:port. Must accept Prometheus queries and OTLP pushes at routes matching VictoriaMetrics.")

	resume = flag.String("resume", "",
		"The resume token that was logged.  Will skip as many requests as possible to avoid duplicate work..")

	incremental = flag.Bool("incremental", true,
		"Query for the last timestamp exported and start scrape from there.")
	incrementalSince = flag.Duration("incremental-since", 90*24*time.Hour,
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

type Config struct {
	Token        oauth2.Token `json:"token,omitempty"`
	ClientID     string       `json:"client_id,omitempty"`
	ClientSecret string       `json:"client_secret,omitempty"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	configDir, err := os.UserConfigDir()
	if err != nil {
		return err
	}

	configDB, err := jsondb.Open[Config](filepath.Join(configDir, "netatmo", "config.json"))
	if err != nil {
		return err
	}

	var exporter expfmt.Encoder
	if *dest != "" {
		r, w, err := os.Pipe()
		if err != nil {
			return err
		}
		g := &errgroup.Group{}
		g.Go(func() error {
			req, err := http.NewRequestWithContext(ctx, "POST", (&url.URL{
				Scheme: "http", Host: *dest, Path: "/api/v1/import/prometheus",
			}).String(), r)
			if err != nil {
				return err
			}
			req.Header.Set("Content-Encoding", "gzip")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			if *verbose {
				dump, err := httputil.DumpResponse(resp, true)
				if err != nil {
					return err
				}
				log.Printf("response:\n%s", dump)
			}
			return nil
		})
		defer func() {
			log.Print("waiting on upload to complete")
			if err := g.Wait(); err != nil {
				log.Fatal(err)
			}
		}()
		defer w.Close()
		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		exporter = expfmt.NewEncoder(gzw, expfmt.NewFormat(expfmt.TypeTextPlain))
	} else {
		exporter = expfmt.NewEncoder(os.Stdout, expfmt.NewFormat(expfmt.TypeTextPlain))
	}
	exporter.Encode(&dto.MetricFamily{
		Metric: []*dto.Metric{{}},
	})

	config := configDB.Data

	client := netatmo.NewClient(ctx, config.ClientID, config.ClientSecret, config.Token,
		func(t *oauth2.Token, err error) error {
			if err == nil {
				configDB.Data.Token = *t
				return configDB.Save()
			}
			return err
		})

	promClient, err := promclient.NewClient(promclient.Config{Address: "http://" + *dest})
	if err != nil {
		return err
	}
	promAPI := promapi.NewAPI(promClient)

	stations, err := client.GetStations(ctx)
	if err != nil {
		return err
	}
	for _, dev := range stations {
		if *verbose {
			log.Printf("exporting device %q", dev.ID)
		}
		commonAttrs := map[string]string{
			"home_id":   dev.HomeID,
			"home_name": dev.HomeName,
		}

		attrs := maps.Clone(commonAttrs)
		maps.Copy(attrs, map[string]string{
			"dev_id":      string(dev.ID),
			"module_name": dev.Name,
			"module_type": string(dev.Type),
			// attribute.Int("firmware", dev.Firmware),
		})
		exportHistory(ctx, client, promAPI, exporter, attrs, dev.ID, "", dev.DataTypes)

		for _, mod := range dev.Modules {
			if *verbose {
				log.Printf("exporting device %q module %q", dev.ID, mod.ID)
			}
			attrs := maps.Clone(commonAttrs)
			maps.Copy(attrs, map[string]string{
				"dev_id":      string(mod.ID),
				"module_name": mod.Name,
				"module_type": string(mod.Type),
				// attribute.Int("firmware", dev.Firmware),
			})
			exportHistory(ctx, client, promAPI, exporter, attrs, dev.ID, mod.ID, mod.DataTypes)
		}
	}
	return nil
}

func exportHistory(
	ctx context.Context,
	client *netatmo.Client, promAPI promapi.API,
	exporter expfmt.Encoder, attrs map[string]string,
	device netatmo.DeviceID, module netatmo.ModuleID,
	dataTypes []netatmo.DataType,
) error {
	var since time.Time
	if *incremental {
		val, _, err := promAPI.Query(ctx,
			fmt.Sprintf("timestamp(netatmo_%s[%s])", strings.ToLower(string(dataTypes[0])), incrementalSince.String()),
			time.Now())
		if err != nil {
			return err
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
			return nil
		}
		sec, err := strconv.Atoi(r[2])
		if err != nil {
			return err
		}
		since = time.Unix(int64(sec), 0)
		*resume = ""
	}

	labels := []*dto.LabelPair{}
	for k, v := range attrs {
		labels = append(labels, &dto.LabelPair{
			Name:  ptr(string(k)),
			Value: ptr(string(v)),
		})
	}

	err := client.GetMeasure(ctx, device, module, dataTypes, since, func(points []netatmo.DataPoint, nextTime time.Time) error {
		// Gauges contain the datapoints.
		for i, dt := range dataTypes {
			// MetricFamily gives the gauges a name and units.
			mf := &dto.MetricFamily{
				Name: ptr("netatmo_" + strings.ToLower(string(dt))),
				Type: dto.MetricType_GAUGE.Enum(),
			}
			for _, point := range points {
				mf.Metric = append(mf.Metric,
					&dto.Metric{
						Label:       labels,
						TimestampMs: proto.Int64(point.Time.UnixMilli()),
						Gauge: &dto.Gauge{
							Value: proto.Float64(point.Values[i]),
						},
					})
			}
			if *verbose {
				log.Printf("Exporting %d datapoints", len(mf.Metric))
			}
			if err := exporter.Encode(mf); err != nil {
				return err
			}
		}

		if *verbose {
			log.Printf("Resume token: %s/%s/%d", device, module, nextTime.Unix())
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func ptr[T any](v T) *T { return &v }
