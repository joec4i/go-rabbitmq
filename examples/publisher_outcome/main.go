package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

// deleteQueues removes leftovers from earlier runs so the example starts from
// a known state: no_such_queue must not exist for the unroutable simulation
// to hold, and my_queue may still hold unconsumed messages. go-rabbitmq has
// no queue admin API, so this uses the underlying amqp client. Deleting a
// queue that doesn't exist is a no-op.
func deleteQueues(url string, names ...string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	for _, name := range names {
		if _, err := ch.QueueDelete(name, false, false, false); err != nil {
			return err
		}
	}
	return nil
}

// This example publishes a batch of messages and collects the ones that need
// to be republished: nacked, returned as unroutable, or lost to a reconnect.
// Confirmations and returns are paired per message by the library, so the
// failed set is exact even with the whole batch in flight: expect the 50
// messages aimed at no_such_queue to fail and the other 450 to be delivered.
func main() {
	const url = "amqp://guest:guest@localhost"

	if err := deleteQueues(url, "no_such_queue", "my_queue"); err != nil {
		log.Fatal(err)
	}

	conn, err := rabbitmq.NewConn(
		url,
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// the consumer declares my_queue and drains it; without the queue every
	// message would be unroutable, not just the no_such_queue ones
	// durable: recent RabbitMQ versions refuse transient non-exclusive queues
	consumer, err := rabbitmq.NewConsumer(conn, "my_queue",
		rabbitmq.WithConsumerOptionsLogging,
		rabbitmq.WithConsumerOptionsQueueDurable,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	go func() {
		if err := consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
			return rabbitmq.Ack
		}); err != nil {
			log.Println("consumer stopped:", err)
		}
	}()
	// give the consumer a moment to declare the queue before publishing
	time.Sleep(time.Second)

	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsConfirm,
		// bounds memory: at most 64 messages awaiting their outcome at once
		rabbitmq.WithPublisherOptionsMaxOutcomesInFlight(64),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var outcomes []*rabbitmq.PublishOutcome
	for i := range 500 {
		routingKey := "my_queue"
		if i%10 == 0 {
			routingKey = "no_such_queue" // simulate unroutable messages
		}
		batch, err := publisher.PublishWithOutcome(
			ctx,
			[]byte(fmt.Sprintf("message %d", i)),
			[]string{routingKey},
			// mandatory is required for the broker to return unroutable
			// messages instead of silently dropping them
			rabbitmq.WithPublishOptionsMandatory,
		)
		if err != nil {
			log.Fatalf("publish %d: %v", i, err)
		}
		outcomes = append(outcomes, batch...)
	}

	var failed []rabbitmq.Outcome
	for _, po := range outcomes {
		outcome, err := po.Wait(ctx)
		if err != nil {
			log.Fatalf("waiting for outcomes: %v", err)
		}
		if outcome.Failed() {
			failed = append(failed, outcome)
		}
	}

	log.Printf("%d of %d messages need to be republished", len(failed), len(outcomes))
	for _, outcome := range failed {
		switch {
		case outcome.Err != nil:
			// channel was lost mid-flight: the message may have been
			// delivered, republish only if consumers deduplicate
			log.Printf("outcome unknown for key %s (tag %d)", outcome.RoutingKey, outcome.DeliveryTag)
		case outcome.Return != nil:
			log.Printf("unroutable: key %s, reply %s", outcome.Return.RoutingKey, outcome.Return.ReplyText)
		default:
			log.Printf("nacked by broker: key %s (tag %d)", outcome.RoutingKey, outcome.DeliveryTag)
		}
	}
}
