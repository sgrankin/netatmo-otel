package netatmo

import (
	"encoding/json"
)

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

type genericResponse struct {
	Body  json.RawMessage `json:"body"`
	Error json.RawMessage `json:"error"`
}

type errorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type getStationsBody struct {
	Stations []Station `json:"devices"`
}

type Station struct {
	ID              DeviceID   `json:"_id"`
	Type            ModuleType `json:"type"` // NAMain
	LastStatusStore unixTime   `json:"last_status_store"`
	Name            string     `json:"module_name"`
	Firmware        int        `json:"firmware"`
	Reachable       bool       `json:"reachable"`
	CO2Calibrating  bool       `json:"co2_calibrating"`

	HomeID   string `json:"home_id"`
	HomeName string `json:"home_name"`

	DataTypes     []DataType    `json:"data_type"`
	DashboardData DashboardData `json:"dashboard_data"`

	Modules []struct {
		ID             ModuleID   `json:"_id"`
		Type           ModuleType `json:"type"` // NAModuleN
		Name           string     `json:"module_name"`
		Reachable      bool       `json:"reachable"`
		Firmware       int        `json:"firmware"`
		BatteryVP      int        `json:"battery_vp"`
		BatteryPercent int        `json:"battery_percent"`

		DataTypes     []DataType    `json:"data_type"`
		DashboardData DashboardData `json:"dashboard_data"`
	}
}

type DashboardData struct {
	TimeUTC          unixTime `json:"time_utc"`
	Temperature      *float64
	CO2              *float64
	Humidity         *float64
	Noise            *float64
	Pressure         *float64
	AbsolutePressure *float64
}

type getMeasureBody []struct {
	Time  unixTime    `json:"beg_time"`
	Step  int         `json:"step_time"`
	Value [][]float64 `json:"value"`
}
