package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/segmentio/kafka-go"
)

var COLUMNS = []string{
	"WILDFIRE_TYPE", "BURNED_POPULATIONAL_AREA", "BURNED_BRUSHLAND_AREA", "BURNED_AGRICULTURAL_AREA", "BURNED_TOTAL_AREA", "HECTARES_PER_HOUR", "LATITUDE", "LONGITUDE", "TEMPERATURE_CELSIUS", "RELATIVE_HUMIDITY_PERCENT", "WIND_SPEED_MS", "PRECIPITATION_MM", "FWI", "MEAN_ALTITUDE_M", "MEAN_SLOPE_DEG", "VEGETATION_DENSITY", "VEGETATION_VARIETY_INDEX", "ALERT_TIMESTAMP", "FIRST_INTERVENTION_TIMESTAMP", "EXTINCTION_TIMESTAMP",
}
var COLUMNS_MAP = make(map[string]int)

// columns with string values
var STRING_COLUMN = "WILDFIRE_TYPE"

// columns with timestamp have TIMESTAMP in their name

// columns with boolean values start with IS_
func convertToInfluxPoint(fields []string) *write.Point {
	// create a new point
	// Find the index of "ALERT_TIMESTAMP"
	alertIdx := -1
	for i, col := range COLUMNS {
		if col == "ALERT_TIMESTAMP" {
			alertIdx = i
			break
		}
	}
	var pointTime time.Time
	if alertIdx != -1 && fields[alertIdx] != "" {
		tsFloat, err := strconv.ParseFloat(fields[alertIdx], 64)
		if err == nil {
			pointTime = time.Unix(int64(tsFloat), 0)
		}
	} else {
		return nil
	}

	p := influxdb2.NewPoint("wildfire",
		map[string]string{},
		map[string]any{},
		pointTime,
	)

	for i, field := range fields {
		colName := COLUMNS[i]
		if field == "" {
			// skip empty fields, do not add as NaN
			continue
		}
		if colName == STRING_COLUMN {
			p.AddTag(colName, field)
		} else if strings.HasPrefix(colName, "IS_") {
			if field == "1" {
				p.AddField(colName, true)
			}
		} else if strings.Contains(colName, "TIMESTAMP") {
			// convert to float then to int64
			tsFloat, err := strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("Failed to parse timestamp %s: %v", field, err)
				continue
			}
			t := time.Unix(int64(tsFloat), 0)
			p.AddField(colName, t)
		} else {
			// convert to float
			value, err := strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("Failed to parse float %s: %v", field, err)
				continue
			}
			p.AddField(colName, value)
		}
	}
	p.AddField("count", 1)
	return p
}

func main() {
	// Wait for Kafka to be available (up to 120s)
	kafkaReady := false
	for range 120 { // up to 120 seconds
		conn, err := kafka.Dial("tcp", "kafka:9092")
		if err == nil {
			conn.Close()
			kafkaReady = true
			break
		}
		log.Printf("Waiting for Kafka to be ready: %v", err)
		time.Sleep(1 * time.Second)
	}
	if !kafkaReady {
		log.Fatalf("Kafka not available after waiting")
	}

	// connect to kafka topic "wildfires" and consume messages
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		Topic:     "wildfires",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	// Wait for influxdb token file to exist
	tokenPath := "/shared/influxdb.token"
	for range 120 { // up to 120 seconds
		if _, err := os.Stat(tokenPath); err == nil {
			break
		}
		log.Printf("Waiting for influxdb token file: %s", tokenPath)
		time.Sleep(1 * time.Second)
	}
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		log.Fatalf("Could not read influxdb token file after waiting: %v", err)
	}
	influxDB := influxdb2.NewClient("http://influxdb:8086", strings.TrimSpace(string(token)))
	defer influxDB.Close()

	// create a new write API
	writeAPI := influxDB.WriteAPIBlocking("UvA", "wildfire_bucket")

	// consume messages from kafka
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		// parse message by splitting by comma
		fields := strings.Split(string(m.Value), ",")
		if len(fields) != len(COLUMNS) {
			log.Printf("Invalid message: %s", string(m.Value))
			continue
		}

		point := convertToInfluxPoint(fields)
		if point == nil {
			log.Printf("Failed to convert message to point: %s", string(m.Value))
			continue
		}

		// write point to influxdb
		err = writeAPI.WritePoint(context.Background(), point)
		if err != nil {
			log.Printf("Failed to write point to influxdb: %v", err)
		} else {
			log.Printf("Wrote point to influxdb: measurement=%s tags=%v fields=%v time=%v",
				point.Name(), point.TagList(), point.FieldList(), point.Time())
		}
	}
}
