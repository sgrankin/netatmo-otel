package netatmo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/time/rate"
)

type Client struct {
	baseURL string
	client  *http.Client
}

func NewClient(ctx context.Context,
	clientID, clientSecret string, token oauth2.Token,
	newToken func(*oauth2.Token, error) error,
) *Client {
	baseURL := "https://api.netatmo.net"
	oa := oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       []string{"read_station"},
		Endpoint:     oauth2.Endpoint{AuthURL: baseURL + "/oauth/authorize", TokenURL: baseURL + "/oauth2/token"},
	}

	throttledClient := &http.Client{Transport: &throttledTransport{http.DefaultTransport,
		rate.NewLimiter(rate.Limit(300.0/3600), 50), // 500 per hour, 50 per 10s; reduced for convenience.
	}}

	ts := oauth2.ReuseTokenSource(nil, &NotifyingTokenSource{oa.TokenSource(ctx, &token), newToken})
	ctx = context.WithValue(ctx, oauth2.HTTPClient, throttledClient)
	return &Client{baseURL: baseURL, client: oauth2.NewClient(ctx, ts)}
}

type NotifyingTokenSource struct {
	oauth2.TokenSource
	Notify func(*oauth2.Token, error) error
}

// Token implements oauth2.TokenSource.
func (c *NotifyingTokenSource) Token() (*oauth2.Token, error) {
	tok, err := c.TokenSource.Token()
	err = c.Notify(tok, err)
	return tok, err
}

func (c *Client) GetStations(ctx context.Context) ([]Station, error) {
	body, err := doRequest[getStationsBody](ctx, c.client, c.baseURL+"/api/getstationsdata")
	if err != nil {
		return nil, err
	}
	return body.Stations, err
}

type DataPoint struct {
	Time   time.Time
	Values []float64
}

// GetMeasure paginates through the module data for the given dataTypes, starting at since.
//
// It yields pages of data after each request,and the next timestamp that will be used (for resuming).
func (c *Client) GetMeasure(
	ctx context.Context, device DeviceID, module ModuleID, dataTypes []DataType, since time.Time,
	yield func(points []DataPoint, nextTime time.Time) error,
) error {
	v := url.Values{}
	v.Set("device_id", string(device))
	if module != "" {
		v.Set("module_id", string(module))
	}
	v.Set("scale", "max") // Use maximum resolution.
	v.Set("type", joinStrings(dataTypes, ","))
	v.Set("optimize", "true")  // Use compact result format.
	v.Set("real_time", "true") // Probably does nothing.
	if !since.IsZero() {
		v.Set("date_begin", fmt.Sprintf("%d", since.Unix()))
	}

	for {
		body, err := doRequest[getMeasureBody](ctx, c.client, c.baseURL+"/api/getmeasure?"+v.Encode())
		if err != nil {
			return err
		}
		if len(body) == 0 {
			return nil // No data; we're done.
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
		if err := yield(points, t); err != nil {
			return err
		}
		v.Set("date_begin", fmt.Sprintf("%d", t.Add(time.Second).Unix()))
	}
}

// doRequest GETs the given URL and on success decodes the JSON body as T.
func doRequest[T any](ctx context.Context, client *http.Client, url string) (T, error) {
	var zero T
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return zero, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		dump, _ := httputil.DumpResponse(resp, true)
		return zero, fmt.Errorf("code: %d; body: %s", resp.StatusCode, dump)
	}

	var r genericResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return zero, err
	}

	if r.Error != nil {
		var er errorBody
		if err := json.Unmarshal(r.Error, &er); err != nil {
			return zero, err
		}
		return zero, err
	}

	var body T
	if err := json.Unmarshal(r.Body, &body); err != nil {
		return zero, err
	}
	return body, nil
}

// joinStrings is strings.Join that accepts types defined as string.
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

// throttledTransport is an http.RoundTripper that waits on a built in rate.Limiter for each request.
type throttledTransport struct {
	http.RoundTripper
	Limiter *rate.Limiter
}

func (t *throttledTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.Limiter.Wait(req.Context()); err != nil {
		return nil, fmt.Errorf("limiter: %w", err)
	}
	return t.RoundTripper.RoundTrip(req)
}

// unixTime marshals time.Time as number  as unix epoch seconds.
type unixTime struct{ time.Time }

func (t *unixTime) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var ts int64
	if err := json.Unmarshal(data, &ts); err != nil {
		return err
	}
	*t = unixTime{time.Unix(ts, 0)}
	return nil
}

func (t *unixTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Unix())
}
