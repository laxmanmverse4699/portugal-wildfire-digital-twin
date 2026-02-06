package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func sendToKafka(writer *kafka.Writer, fields []string) error {
	message := strings.Join(fields, ",")
	return writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(message),
		},
	)
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

	// Ensure Kafka topic exists
	ensureTopic("kafka:9092", "wildfires")

	// Connect to Kafka
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"kafka:9092"},
		Topic:    "wildfires",
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	filePath := os.Getenv("CSV_PATH")
	if filePath == "" {
		filePath = "../../Data_Preprocess/ICNF_2013_2022_cleaned.csv"
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	columns, err := reader.Read()
	if err != nil {
		log.Fatalf("Failed to read CSV file: %v", err)
	}

	// turn columns into a map
	columnsMap := make(map[string]int)
	for i, col := range columns {
		columnsMap[col] = i
	}

	if len(os.Args) < 2 {
		log.Fatalf("Please provide a start date argument in format DD-MM-YYYY")
	}
	startDateStr := os.Args[1]
	timeSpeed, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Invalid timeSpeed argument: %v", err)
	}

	// Parse input date string to time.Time
	startDate, err := time.Parse("02-01-2006", startDateStr)
	if err != nil {
		log.Fatalf("Error parsing date: %v", err)
	}
	currTimestamp := startDate.Unix() // seconds since epoch

	// start with the correct line
	for {
		line, err := reader.Read()
		if err != nil {
			break
		}
		alertTimestampStr := line[columnsMap["ALERT_TIMESTAMP"]]
		alertTimestamp, err := strconv.ParseFloat(alertTimestampStr, 64)
		if err != nil {
			continue // skip invalid lines
		}
		if int64(alertTimestamp) >= currTimestamp {
			fmt.Println("Found line:", line)
			err = sendToKafka(writer, line)
			if err != nil {
				log.Printf("Failed to send message to Kafka: %v", err)
			}
			break
		}
	}
	// infinite loop where the timestamp is increased in real time * timeSpeed (integer) and lines are printed when the ALERT_TIMESTAMP is reached
	printed := false
	for {
		line, err := reader.Read()
		if err != nil {
			break
		}
		alertTimestampStr := line[columnsMap["ALERT_TIMESTAMP"]]
		alertTimestamp, err := strconv.ParseFloat(alertTimestampStr, 64)
		if err != nil {
			continue // skip invalid lines
		}
		printed = false
		for int64(alertTimestamp) > currTimestamp {
			time.Sleep(1 * time.Millisecond)
			currTimestamp += int64(timeSpeed) / 1000
			if !printed && int64(alertTimestamp) <= currTimestamp {
				fmt.Println("Reached line:", line)
				err = sendToKafka(writer, line)
				if err != nil {
					log.Printf("Failed to send message to Kafka: %v", err)
				}
				printed = true
			}
		}
		if !printed && int64(alertTimestamp) <= currTimestamp {
			fmt.Println("Reached line:", line)
			err = sendToKafka(writer, line)
			if err != nil {
				log.Printf("Failed to send message to Kafka: %v", err)
			}
		}
	}

}

func ensureTopic(brokerAddress, topic string) {
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		log.Printf("Failed to dial Kafka broker: %v", err)
		return
	}
	defer conn.Close()
	// controller variable removed; using static controller address
	// Always use the Docker Compose service name for controller connection
	controllerAddr := "kafka:9092"
	controllerConn, err := kafka.Dial("tcp", controllerAddr)
	if err != nil {
		log.Printf("Failed to dial Kafka controller: %v", err)
		return
	}
	defer controllerConn.Close()
	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Printf("Could not create topic (may already exist): %v", err)
	} else {
		log.Printf("Kafka topic '%s' ensured.", topic)
	}
}
