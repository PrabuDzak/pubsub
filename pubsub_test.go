package pubsub_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/PrabuDzak/pubsub"
)

func TestPubSubExample(t *testing.T) {
	ps := pubsub.New()

	subs1, err := ps.Subscribe("test-topic-1")
	if err != nil {
		t.Fatal(err)
	}

	subs2, err := ps.Subscribe("test-topic-2")
	if err != nil {
		t.Fatal(err)
	}

	subs3, err := ps.Subscribe("test-topic-2")
	if err != nil {
		t.Fatal(err)
	}

	go printSubscribedTopic(subs1, "subs-1")
	go printSubscribedTopic(subs2, "subs-2")
	go printSubscribedTopic(subs3, "subs-3")

	for i := 0; i < 10; i++ {
		ps.Publish("test-topic-1", fmt.Sprintf("message-%d", i))
		ps.Publish("test-topic-2", fmt.Sprintf("text-%d", i))
	}

	time.Sleep(100 * time.Millisecond)

	subs1.Close()
	subs2.Close()
	subs3.Close()
}

func printSubscribedTopic(subs pubsub.Subscriber, prefix string) {
	for {
		message, ok := <-subs.Listen()
		if !ok {
			break
		}
		fmt.Println(prefix, message)
	}
}
