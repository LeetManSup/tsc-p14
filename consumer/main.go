package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		GroupID: "loggers",
	})
	defer reader.Close()

	fmt.Println("Ожидаем события из Kafka...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("Ошибка чтения:", err)
			continue
		}
		fmt.Printf("Получено: %s\n", string(msg.Value))
	}
}
