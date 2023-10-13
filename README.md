Export netatmo weather data into VictoriaMetrics or other OpenTelemetry destinations.

- https://www.netatmo.com

To access the API, create an application in Netatmo Connect and generate a token with a `read_station` scope.

- https://dev.netatmo.com
- https://dev.netatmo.com/apidocumentation/oauth

Pass the tokens via flags, environment, or config file. (See `-help`.)

The destination host is expected to be VictoriaMetrics: the OTLP routes are used for teh data export, and the Prometheus query routes are used to check what the last sample written was (for incremental sends).

Run as a cron job every 5 minutes; that's the frequency the stations will upload at. Mind the rate limits.

- https://dev.netatmo.com/guideline#rate-limits
