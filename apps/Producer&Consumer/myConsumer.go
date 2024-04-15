package main

import (
	"github.com/nsqio/go-nsq"
	"log"
)

var sum int

func main() {

	//设置日志头
	log.SetPrefix("[consumer_log]:")
	sum = 0

	//新建消费者
	cfg := nsq.NewConfig()
	cfg.Set("lookupd_poll_jitter", 0)
	cfg.Set("nsqd_retry_times", 6)
	consumer, err := nsq.NewConsumer("test", "c1", cfg)
	if err != nil {
		log.Fatal(err)
	}
	//添加消息处理器
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		sum = sum + 1
		log.Printf("Received message num :%d , message is :%s", sum, message.Body)

		message.Finish()
		return nil // 表示消息已被正确处理
	}))
	//消费者连接nsqlookupd
	nsqlookupds := []string{"127.0.0.1:4161"}
	if err := consumer.ConnectToNSQLookupds(nsqlookupds); err != nil {
		log.Fatal(err)
	}
	log.Printf("consumer started")

	select {}

}
