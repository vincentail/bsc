package kafka

import (
	"github.com/IBM/sarama"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/log"
	"strings"
)

// Backend wraps all methods required for mining. Only full node is capable
// to offer all the functions here.
type Backend interface {
	TxPool() *txpool.TxPool
}

// Miner is the main object which takes care of submitting new work to consensus
// engine and gathering the sealing result.
type Kafka struct {
	broker   []string
	topic    string
	group    string
	consumer sarama.Consumer
	eth      Backend
	startCh  chan struct{}
}

func New(eth Backend, broker string, topic string, group string) (*Kafka, error) {
	kafka := &Kafka{
		broker:  strings.Split(broker, ","),
		topic:   topic,
		group:   group,
		eth:     eth,
		startCh: make(chan struct{}),
	}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true // 让错误可以通过 consumer.Errors() 读取
	config.Version = sarama.V2_8_1_0
	consumer, err := sarama.NewConsumer(kafka.broker, config)
	if err != nil {
		log.Error("Failed to create kafka consumer", "err", err)
		return nil, err
	}
	kafka.consumer = consumer
	return kafka, nil
}

// update keeps track of the downloader events. Please be aware that this is a one shot type of update loop.
// It's entered once and as soon as `Done` or `Failed` has been broadcasted the events are unregistered and
// the loop is exited. This to prevent a major security vuln where external parties can DOS you with blocks
// and halt your mining operation for as long as the DOS continues.
func (kafka *Kafka) startConsumer() {
	defer kafka.consumer.Close()

	kafka.consumer.ConsumePartition(kafka.topic, 0, sarama.OffsetNewest)
}
