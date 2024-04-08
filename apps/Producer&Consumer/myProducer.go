package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

func main() {

	log.SetPrefix("[producer_log]:")
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer("localhost:4250", config)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Stop()
	var sum int = 0
	log.Printf("producer start !")

	for {
		err := producer.Publish("test_topic", []byte(fmt.Sprintf("[Message %d]", sum)))
		if err != nil {
			log.Printf("Failed to publish message")
		} else {
			log.Printf("No.%d message Published success !", sum)
		}
		sum = sum + 1
		// 延迟1秒
		time.Sleep(1 * time.Second)
	}

}
