package service

import (
	"datastream/config"
	"datastream/logs"
	"time"

	"strings"

	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
)

func SendToKafka(data []string,
	kafkaConnector *config.KafkaConnector, topic string) error {

	for _, contact := range data {

		// Send the message to the specified topic
		err := kafkaConnector.SendMessage(topic, contact)
		if err != nil {

			logs.Logger.Error("Error sending message: ", err)
		}
	}
	return nil
}

func ConsumeMessages(brokerList string, topic string, messageChannel chan string) {

	partitionConsumer, consumer, err := ConsumePartition(topic, 0, sarama.OffsetOldest, brokerList)

	if err != nil {
		logs.Logger.Info("Failed to create partition consumer: %v\n", err)
	}

	defer partitionConsumer.Close()
	defer consumer.Close()

	inactivityTimer := time.NewTimer(100 * time.Second)
	inactivityTimer.Stop()
	for {
		select {
		case <-inactivityTimer.C:
			return
		case msg := <-partitionConsumer.Messages():

			inactivityTimer.Reset(100 * time.Second)

			message := string(msg.Value)

			messageChannel <- message
		}
	}
}

func ConsumePartition(topic string, partition int32, offset int64, brokerList string) (sarama.PartitionConsumer, sarama.Consumer, error) {

	brokers := strings.Split(brokerList, ",")

	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = offset

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, nil, err
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)

	if err != nil {
		panic(err)
	}

	return partitionConsumer, consumer, nil
}
