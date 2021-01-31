package mqttutil

import (
	"errors"
	"strings"
	"sync"
)

var (
	ErrInvalidTopic           = errors.New("Invalid topic")
	ErrEmptySubscriptionTopic = errors.New("Empty subscription topics are not allowed")
)

// ValidatePublishTopic that a topic used for publishing is valid.
// Return ErrInvalidTopic if + or # found in the topic
func ValidatePublishTopic(topic string) error {
	if len(topic) > 65535 {
		return ErrInvalidTopic
	}

	if strings.ContainsAny(topic, "+#") {
		return ErrInvalidTopic
	}

	return nil
}

// ValidateSubscribeTopic Validate that a topic used for
// subscriptions is valid. Search for + or # in a topic,
// validate that they are not in the invalid positions
func ValidateSubscribeTopic(topic string) error {
	if len(topic) == 0 {
		return ErrEmptySubscriptionTopic
	}

	if len(topic) > 65535 {
		return ErrInvalidTopic
	}

	var previousChar rune
	topicLen := len(topic)

	for i, c := range topic {
		if c == '+' {
			if (i != 0 && previousChar != '/') || (i < topicLen-1 && topic[i+1] != '/') {
				return ErrInvalidTopic
			}
		} else if c == '#' {
			if (i != 0 && previousChar != '/') || (i < (topicLen - 1)) {
				return ErrInvalidTopic
			}
		}
		previousChar = c
	}
	return nil
}

func isEmpty(str string) bool {
	return len(str) == 0
}

// Subscriber ...
type Subscriber interface {
	Finalize()
}

type node struct {
	part       string
	subscriber Subscriber
	parent     *node
	children   map[string]*node
}

func (n *node) remove() {
	if n.parent == nil {
		// root node
		return
	}

	delete(n.parent.children, n.part)
	if n.parent.subscriber != nil && len(n.parent.children) == 0 {
		n.parent.remove()
	}
}

// TopicMatcher ...
type TopicMatcher struct {
	root *node
	mu   sync.RWMutex
}

// NewTopicMatcher new topic matcher
func NewTopicMatcher() *TopicMatcher {
	return &TopicMatcher{
		root: &node{
			children: make(map[string]*node),
		},
	}
}

// Subscribe ...
func (t *TopicMatcher) Subscribe(topic string, sub Subscriber) error {
	if err := ValidateSubscribeTopic(topic); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	cur := t.root
	for _, part := range strings.Split(topic, "/") {
		child, ok := cur.children[part]
		if !ok {
			child = &node{
				part:     part,
				parent:   cur,
				children: make(map[string]*node),
			}
			cur.children[part] = child
		}
		cur = child
	}

	cur.subscriber = sub

	return nil
}

// Unsubscribe ...
func (t *TopicMatcher) Unsubscribe(topic string) error {
	if err := ValidateSubscribeTopic(topic); err != nil {
		return err
	}

	var subscriber Subscriber
	t.mu.Lock()
	cur := t.root
	for _, part := range strings.Split(topic, "/") {
		child, ok := cur.children[part]
		if !ok {
			// no subscription
			return nil
		}
		cur = child
	}

	subscriber = cur.subscriber
	cur.subscriber = nil

	// check wheher we have children
	if len(cur.children) == 0 {
		cur.remove()
	}
	t.mu.Unlock()

	if subscriber != nil {
		subscriber.Finalize()
	}

	return nil
}

// Match ...
func (t *TopicMatcher) Match(topic string, subscribers *[]Subscriber) error {
	if err := ValidatePublishTopic(topic); err != nil {
		return nil
	}

	if subscribers == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.match(strings.Split(topic, "/"), t.root, subscribers)

	return nil
}

func addSubscriber(sub Subscriber, subscribers *[]Subscriber) {
	if sub != nil {
		*subscribers = append(*subscribers, sub)
	}
}

func (t *TopicMatcher) match(parts []string, node *node, subscribers *[]Subscriber) {
	// "foo/#” also matches the singular "foo", since # includes the parent level.
	if n, ok := node.children["#"]; ok {
		addSubscriber(n.subscriber, subscribers)
	}

	if len(parts) == 0 {
		addSubscriber(node.subscriber, subscribers)
		return
	}

	if n, ok := node.children["+"]; ok {
		// found +, check it is the last part
		// from MQTTv5 spec
		// e.g “sport/tennis/+” matches “sport/tennis/player1” and “sport/tennis/player2”,
		// but not “sport/tennis/player1/ranking”.
		if len(parts) == 1 {
			addSubscriber(n.subscriber, subscribers)
			t.match(parts, n, subscribers)
		} else {
			t.match(parts[1:], n, subscribers)
		}
	}
	if n, ok := node.children[parts[0]]; ok {
		t.match(parts[1:], n, subscribers)
	}
}
