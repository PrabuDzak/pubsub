package pubsub

import (
	"errors"
	"sync"
)

// PubSub represent publisher/subscriber messaging
type PubSub struct {
	topics      map[string]*topic
	publishChan chan *payload
	closingChan chan *subscriber
	topicMutex  sync.RWMutex
}

// New create a new PubSub instance
func New() *PubSub {
	pubsub := &PubSub{
		topics:      make(map[string]*topic),
		publishChan: make(chan *payload),
		closingChan: make(chan *subscriber),
	}

	go pubsub.pushPublished()
	go pubsub.closing()

	return pubsub
}

func (p *PubSub) createTopic(name string) *topic {
	p.topicMutex.Lock()
	defer p.topicMutex.Unlock()

	t, ok := p.topics[name]
	if ok {
		return t
	}

	t = &topic{name: name, subscribers: &sync.Map{}}
	p.topics[name] = t
	return t
}

// Publisher represent a topic publisher interface
type Publisher interface {
	// Publish publish a message to a topic. Message published will be recevied by all
	// topic subscribers if any
	Publish(topic string, message interface{}) error
}

var _ Publisher = (*PubSub)(nil)

// Publish publish a message to a topic. Message published will be recevied by all
// topic subscribers if any
func (p *PubSub) Publish(topic string, message interface{}) error {
	if topic == "" {
		return errors.New("topic parameter is empty")
	}

	if message == nil {
		return errors.New("message is nil")
	}

	p.publishChan <- &payload{topic: topic, message: message}
	return nil
}

// Subscribe create a new subcriber that will receive message from a topic
func (p *PubSub) Subscribe(topic string) (Subscriber, error) {
	if topic == "" {
		return nil, errors.New("topic parameter is empty")
	}

	p.topicMutex.RLock()
	t, ok := p.topics[topic]
	p.topicMutex.RUnlock()
	if !ok {
		t = p.createTopic(topic)
	}

	subscriber := newSubscriber(topic, p.closingChan)
	t.subscribers.Store(subscriber, subscriber)

	return subscriber, nil
}

func (p *PubSub) closing() {
	for closingSubscriber := range p.closingChan {
		p.topics[closingSubscriber.topic].subscribers.Delete(closingSubscriber)
		closingSubscriber.close()
	}
}

func (p *PubSub) pushPublished() {
	for published := range p.publishChan {
		p.topicMutex.RLock()
		t, ok := p.topics[published.topic]
		p.topicMutex.RUnlock()
		if !ok {
			continue
		}

		t.subscribers.Range(func(key, value interface{}) bool {
			subscriber, ok := value.(*subscriber)
			if !ok {
				return true
			}

			subscriber.receive(published.message)
			return true
		})
	}
}

// Subscriber represent a topic subscriber interface
type Subscriber interface {
	// Listen return a channel receiving messages from topic publisher
	Listen() chan interface{}
	// Close mark a subscriber closed, done listening and will close Subcriber channel
	Close()
}

var _ Subscriber = (*subscriber)(nil)

type subscriber struct {
	topic       string
	listenChan  chan interface{}
	closingChan chan *subscriber
	mutex       *sync.RWMutex
	closed      bool
}

func newSubscriber(topic string, closingChan chan *subscriber) *subscriber {
	return &subscriber{
		topic:       topic,
		listenChan:  make(chan interface{}),
		closingChan: closingChan,
		mutex:       &sync.RWMutex{},
	}
}

func (s *subscriber) Listen() chan interface{} {
	return s.listenChan
}

func (s *subscriber) Close() {
	if s.closed {
		return
	}

	s.closed = true
	s.closingChan <- s
}

func (s *subscriber) close() {
	s.mutex.Lock()
	close(s.listenChan)
	s.mutex.Unlock()
}

func (s *subscriber) receive(message interface{}) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return
	}

	s.listenChan <- message
}

type topic struct {
	name        string
	subscribers *sync.Map
}

type payload struct {
	topic   string
	message interface{}
}
