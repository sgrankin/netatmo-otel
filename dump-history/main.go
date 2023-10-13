package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/peterbourgon/ff/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/oauth2"
	"golang.org/x/time/rate"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

const (
	baseURL = "https://api.netatmo.net"
)

var (
	fs = flag.NewFlagSet("flags", flag.ExitOnError)

	accessToken  = fs.String("access-token", "", "Oauth2 Access Token")
	refreshToken = fs.String("refresh-token", "", "Oauth2 Refresh Token")
	clientID     = fs.String("client-id", "", "Oauth2 Client ID")
	clientSecret = fs.String("client-secret", "", "Oauth2 Client Secret")

	dest  = fs.String("dest", "", "destination host:port")
	debug = fs.Bool("debug", false, "Log debug info")

	resume = fs.String("resume", "", "Resume token")

	ago         = fs.Duration("ago", 7*24*time.Hour, "Start scrape this long ago.  If incremental, look back this far to find the latest date.")
	incremental = fs.Bool("incremental", true, "Check for the last timestamp exported and read from there")
)

func init() {
	if err := ff.Parse(fs, os.Args[1:], ff.WithEnvVars()); err != nil {
		log.Fatal(err)
	}
}

func main() {
	ctx := context.Background()
	oa := oauth2.Config{
		ClientID:     *clientID,
		ClientSecret: *clientSecret,
		Scopes:       []string{"read_station"},
		Endpoint:     oauth2.Endpoint{AuthURL: baseURL + "/oauth/authorize", TokenURL: baseURL + "/oauth2/token"},
	}

	client := Client{
		BaseURL: baseURL,
		HTTPClient: oa.Client(
			context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
				Transport: &ThrottledTransport{http.DefaultTransport,
					rate.NewLimiter(rate.Limit(400.0/3600), 50)}}),
			&oauth2.Token{AccessToken: *accessToken, RefreshToken: *refreshToken, Expiry: time.Now()}),
	}
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

	stations := client.GetStations(ctx)
	for _, dev := range stations.Devices {
		log.Printf("exporting device %q", dev.ID)
		attrs := attribute.NewSet(
			attribute.String("dev_id", string(dev.ID)),
			attribute.String("home_id", dev.HomeID),
			attribute.String("home_name", dev.HomeName),
			attribute.String("module_name", dev.Name),
			attribute.String("module_type", string(dev.Type)),
			// attribute.Int("firmware", dev.Firmware),
		)
		client.exportHistory(ctx, exporter, attrs, dev.ID, nil, dev.DataTypes)

		for _, mod := range dev.Modules {
			log.Printf("exporting device %q module %q", dev.ID, mod.ID)
			attrs := attribute.NewSet(
				attribute.String("dev_id", string(mod.ID)),
				attribute.String("home_id", dev.HomeID),
				attribute.String("home_name", dev.HomeName),
				attribute.String("module_name", mod.Name),
				attribute.String("module_type", string(mod.Type)),
				// attribute.Int("firmware", dev.Firmware),
			)
			client.exportHistory(ctx, exporter, attrs, dev.ID, &mod.ID, mod.DataTypes)
		}
	}

}

func (c *Client) exportHistory(
	ctx context.Context,
	exporter exporter,
	attrs attribute.Set,
	device DeviceID, module *ModuleID,
	dataTypes []DataType,
) {
	var since time.Time
	if *incremental {
		promClient, err := api.NewClient(api.Config{
			Address: "http://" + *dest,
		})
		if err != nil {
			log.Fatal(err)

		}

		prom := v1.NewAPI(promClient)
		val, _, err := prom.Query(ctx,
			fmt.Sprintf("timestamp(netatmo_%s[%s])", strings.ToLower(string(dataTypes[0])), ago.String()),
			time.Now())
		if err != nil {
			log.Fatal(err)
		}
		vec := val.(model.Vector)
		for _, sample := range vec {
			if module != nil && string(sample.Metric["dev_id"]) == string(*module) ||
				module == nil && string(sample.Metric["dev_id"]) == string(device) {
				since = time.Unix(int64(sample.Value), 0).Add(time.Second)
				break
			}
		}
	}

	c.GetMeasure(ctx, device, module, dataTypes, since, func(points []DataPoint) {
		gauges := make([]metricdata.Gauge[float64], len(dataTypes))
		for _, point := range points {
			for i := range dataTypes {
				gauges[i].DataPoints = append(gauges[i].DataPoints, metricdata.DataPoint[float64]{
					Attributes: attrs,
					Time:       point.Time,
					Value:      point.Values[i]},
				)
			}
		}
		metrics := []metricdata.Metrics{}
		for i, gauge := range gauges {
			dt := dataTypes[i]
			metrics = append(metrics, metricdata.Metrics{
				Name: "netatmo_" + strings.ToLower(string(dt)),
				Data: gauge,
				Unit: DataUnits[dt]},
			)
		}
		log.Printf("exporting %d datapoints", len(gauges[0].DataPoints))
		if err := exporter.Export(ctx, &metricdata.ResourceMetrics{ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: metrics}}}); err != nil {
			log.Fatal(err)
		}
		for _, gauge := range gauges {
			gauge.DataPoints = nil
		}
	})
}

type exporter interface {
	Export(ctx context.Context, rm *metricdata.ResourceMetrics) error
}

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

type genericResponse struct {
	Body  json.RawMessage `json:"body"`
	Error json.RawMessage `json:"error"`
}

type errorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type DeviceID string
type ModuleID string
type ModuleType string

const (
	ModuleMain    ModuleType = "NAMain"
	ModuleOutdoor ModuleType = "NAModule1"
	ModuleIndoor  ModuleType = "NAModule4"
)

type DataType string

const (
	DataTemperature DataType = "Temperature"
	DataHumidiity   DataType = "Humidiity"
	DataCO2         DataType = "CO2"
	DataPressure    DataType = "Pressure"
	DataNoise       DataType = "Noise"
	DataRain        DataType = "Rain"
	DataWind        DataType = "Wind"
)

var DataUnits = map[DataType]string{
	DataTemperature: "Cel",
	DataHumidiity:   "%",
	DataCO2:         "[ppm]",
	DataPressure:    "mbar",
	DataNoise:       "dB[SPL]",
	DataRain:        "mm",
	DataWind:        "km/h",
}

type UTCTime struct{ time.Time }

func (t *UTCTime) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var ts int64
	if err := json.Unmarshal(data, &ts); err != nil {
		return err
	}
	*t = UTCTime{time.Unix(ts, 0)}
	return nil
}

func (t *UTCTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Unix())
}

type getStationsBody struct {
	Devices []struct {
		ID              DeviceID   `json:"_id"`
		Type            ModuleType `json:"type"` // NAMain
		LastStatusStore UTCTime    `json:"last_status_store"`
		Name            string     `json:"module_name"`
		Firmware        int        `json:"firmware"`
		Reachable       bool       `json:"reachable"`
		CO2Calibrating  bool       `json:"co2_calibrating"`

		HomeID   string `json:"home_id"`
		HomeName string `json:"home_name"`

		DataTypes     []DataType    `json:"data_type"`
		DashboardData dashboardData `json:"dashboard_data"`

		Modules []struct {
			ID             ModuleID   `json:"_id"`
			Type           ModuleType `json:"type"` // NAModuleN
			Name           string     `json:"module_name"`
			Reachable      bool       `json:"reachable"`
			Firmware       int        `json:"firmware"`
			BatteryVP      int        `json:"battery_vp"`
			BatteryPercent int        `json:"battery_percent"`

			DataTypes     []DataType    `json:"data_type"`
			DashboardData dashboardData `json:"dashboard_data"`
		}
	} `json:"devices"`
}

type dashboardData struct {
	TimeUTC          UTCTime `json:"time_utc"`
	Temperature      *float64
	CO2              *float64
	Humidity         *float64
	Noise            *float64
	Pressure         *float64
	AbsolutePressure *float64
}

func doRequest[T any](ctx context.Context, client *http.Client, url string) (T, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
	}
	if *debug {
		if dump, err := httputil.DumpRequestOut(req, true); err != nil {
			log.Fatal(err)
		} else {
			log.Printf("%s", dump)
		}
	} else {
		log.Printf("request: %q", req.URL)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()
	if *debug {
		if dump, err := httputil.DumpResponse(resp, true); err != nil {
			log.Fatal(err)
		} else {
			log.Printf("%s", dump)
		}
	}

	if resp.StatusCode != http.StatusOK {
		dump, _ := httputil.DumpResponse(resp, true)
		log.Fatalf("code: %d; body: %s", resp.StatusCode, dump)
	}

	var r genericResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		log.Fatal(err)
	}

	if r.Error != nil {
		var er errorBody
		if err := json.Unmarshal(r.Error, &er); err != nil {
			log.Fatal(err)
		}
		log.Fatal(er)
	}

	var body T
	if err := json.Unmarshal(r.Body, &body); err != nil {
		log.Fatal(err)
	}

	return body, nil
}

func (c *Client) GetStations(ctx context.Context) getStationsBody {
	body, err := doRequest[getStationsBody](ctx, c.HTTPClient, c.BaseURL+"/api/getstationsdata")
	if err != nil {
		log.Fatal(err)
	}
	return body
}

type getMeasureBody []struct {
	Time  UTCTime     `json:"beg_time"`
	Step  int         `json:"step_time"`
	Value [][]float64 `json:"value"`
}

type DataPoint struct {
	Time   time.Time
	Values []float64
}

func (c *Client) GetMeasure(ctx context.Context, device DeviceID, module *ModuleID, dataTypes []DataType, since time.Time, yield func(points []DataPoint)) {

	// date_begin=1601801552&device_id=70%3Aee%3A50%3A58%3A80%3Ab6&module_id=02%3A00%3A00%3A58%3A9c%3Af4&
	v := url.Values{}
	v.Set("device_id", string(device))
	if module != nil {
		v.Set("module_id", string(*module))
	}
	v.Set("scale", "max")
	v.Set("type", joinStrings(dataTypes, ","))
	v.Set("optimize", "true")
	v.Set("real_time", "true")
	if *ago != 0 {
		v.Set("date_begin", fmt.Sprintf("%d", time.Now().Add(-*ago).Unix()))
	}

	// Resume token present?
	if *resume != "" {
		r := strings.Split(*resume, "/")
		if r[0] != v.Get("device_id") || r[1] != v.Get("module_id") {
			// Token was given and it has some other module.. probably skip ahead.
			return
		}
		v.Set("date_begin", r[2])
		*resume = ""
	} else if !since.IsZero() {
		v.Set("date_begin", fmt.Sprintf("%d", since.Unix()))
	}

	for {
		log.Printf("Resume token: %s/%s/%s", v.Get("device_id"), v.Get("module_id"), v.Get("date_begin"))
		body, err := doRequest[getMeasureBody](ctx, c.HTTPClient, c.BaseURL+"/api/getmeasure?"+v.Encode())
		if err != nil {
			log.Fatal(err)
		}
		if len(body) == 0 {
			return
		}

		points := []DataPoint{}
		var t time.Time
		for _, group := range body {
			t = group.Time.Time
			for _, point := range group.Value {
				points = append(points, DataPoint{t, point})
				t = t.Add(time.Duration(group.Step) * time.Second)
			}
		}
		yield(points)
		v.Set("date_begin", fmt.Sprintf("%d", t.Add(time.Second).Unix()))
	}
}

func joinStrings[T ~string](elems []T, sep string) string {
	if len(elems) == 0 {
		return ""
	}
	res := string(elems[0])
	for _, s := range elems[1:] {
		res += "," + string(s)
	}
	return res
}

type ThrottledTransport struct {
	http.RoundTripper
	Limiter *rate.Limiter
}

func (t *ThrottledTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.Limiter.Wait(req.Context()); err != nil {
		return nil, err
	}
	return t.RoundTripper.RoundTrip(req)
}
