package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	ensureTopic()
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "orders",
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	events := []string{
		"Order created: #123",
		"Order paid: #123",
		"Order shipped: #123",
	}

	for _, e := range events {
		err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("Order123"),
			Value: []byte(e),
		})
		if err != nil {
			fmt.Println("Ошибка отправки:", err)
		} else {
			fmt.Println("Отправлено:", e)
		}
		time.Sleep(1 * time.Second)
	}
}

func ensureTopic() {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err)
	}

	// подключаемся к контроллеру кластера
	connToController, err := kafka.Dial("tcp", controller.Host+":"+fmt.Sprint(controller.Port))
	if err != nil {
		panic(err)
	}
	defer connToController.Close()

	topic := "orders"
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = connToController.CreateTopics(topicConfigs...)
	if err != nil {
		fmt.Println("Ошибка создания топика (возможно, уже существует):", err)
	} else {
		fmt.Println("Топик создан:", topic)
	}
}
