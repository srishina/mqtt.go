package mqttutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testSubscriber struct {
	value          int
	finalizeCalled bool
}

func (t *testSubscriber) Finalize() {
	t.finalizeCalled = true
}

func TestPublishTopicValidation(t *testing.T) {
	validPublishTopics := []string{
		"pub/topic",
		"pub//topic",
		"pub/ /topic",
	}

	invalidPublishTopics := []string{
		"+pub/topic",
		"pub+/topic",
		"pub/+topic",
		"pub/topic+",
		"pub/topic/+",
		"#pub/topic",
		"pub#/topic",
		"pub/#topic",
		"pub/topic#",
		"pub/topic/#",
		"+/pub/topic",
	}
	for _, topic := range validPublishTopics {
		if ValidatePublishTopic(topic) != nil {
			t.Errorf("publish topic check for %s is failed", topic)
		}
	}

	for _, topic := range invalidPublishTopics {
		if ValidatePublishTopic(topic) == nil {
			t.Errorf("publish topic check for %s is failed", topic)
		}
	}
}

func TestSubscribeTopicValidation(t *testing.T) {
	validSubscribeTopics := []string{
		"sub/topic",
		"sub//topic",
		"sub/ /topic",
		"sub/+/topic",
		"+/+/+",
		"+",
		"sub/topic/#",
		"sub//topic/#",
		"sub/ /topic/#",
		"sub/+/topic/#",
		"+/+/+/#",
		"#",
		"/#",
		"sub/topic/+/#",
	}

	invalidSubscribeTopics := []string{
		"+sub/topic",
		"sub+/topic",
		"sub/+topic",
		"sub/topic+",
		"#sub/topic",
		"sub#/topic",
		"sub/#topic",
		"sub/topic#",
		"#/sub/topic",
		"",
	}

	for _, topic := range validSubscribeTopics {
		if ValidateSubscribeTopic(topic) != nil {
			t.Errorf("subscribe topic check for %s is failed", topic)
		}
	}

	for _, topic := range invalidSubscribeTopics {
		if ValidateSubscribeTopic(topic) == nil {
			t.Errorf("subscribe topic check for %s is failed", topic)
		}
	}
}

func matchHelper(t *testing.T, subscribe, topic string) {
	var topicMatcher = NewTopicMatcher()
	err := topicMatcher.Subscribe(subscribe, &testSubscriber{value: 4})
	assert.NoError(t, err, "topic matcher subscribe failed for %s %s", subscribe, topic)

	var subscribers []Subscriber
	result := topicMatcher.Match(topic, &subscribers)
	assert.Equal(t, 1, len(subscribers), "topic matcher match failed for %s %s %v %v", subscribe, topic, result, subscribers)
}

func TestTopicMatch(t *testing.T) {
	matchHelper(t, "foo/#", "foo/")
	matchHelper(t, "foo/+/#", "foo/bar/baz")
	matchHelper(t, "#", "foo/bar/baz")
	matchHelper(t, "/#", "/foo/bar")

	subscribeTopics := map[string]string{

		"foo/#":       "foo",
		"foo//bar":    "foo//bar",
		"foo//+":      "foo//bar",
		"foo/+/+/baz": "foo///baz",
		"foo/bar/+":   "foo/bar/",
		"foo/bar":     "foo/bar",
		"foo/+":       "foo/bar",
		"foo/+/baz":   "foo/bar/baz",
		"A/B/+/#":     "A/B/B/C",

		"foo/+/#": "foo/bar",
		"#":       "foo/bar/baz",
		"/#":      "/foo/bar",
	}

	i := 0
	for k, v := range subscribeTopics {
		expectedSubValues := []int{
			i + 1,
			i + 2,
		}
		i++
		var topicMatcher = NewTopicMatcher()
		if err := topicMatcher.Subscribe(k, &testSubscriber{value: i}); err != nil {
			return
		}
		if err := topicMatcher.Subscribe(k, &testSubscriber{value: i + 1}); err != nil {
			return
		}
		var subscribers []Subscriber
		result := topicMatcher.Match(v, &subscribers)
		assert.Equal(t, 2, len(subscribers), "topic matcher subscribe failed for %s %s %v", k, v, result)

		for _, sub := range subscribers {
			if s, ok := sub.(*testSubscriber); ok {
				assert.Contains(t, expectedSubValues, s.value)
			} else {
				t.Errorf("invalid subscriber returned for %v\n", sub)
			}
		}
	}
}

func noMatchHelper(t *testing.T, subscribe, topic string) {
	var topicMatcher = NewTopicMatcher()
	if err := topicMatcher.Subscribe(subscribe, nil); err != nil {
		return
	}
	var subscribers []Subscriber

	result := topicMatcher.Match(topic, &subscribers)
	assert.Equal(t, 0, len(subscribers), "topic matcher match failed for %s %s %v subscribers len %d", subscribe, topic, result, len(subscribers))
}

func TestTopicNoMatch(t *testing.T) {
	noMatchHelper(t, "test/6/#", "test/3")
	noMatchHelper(t, "foo/bar", "foo")
	noMatchHelper(t, "foo/+", "foo/bar/baz")
	noMatchHelper(t, "foo/+/baz", "foo/bar/bar")
	noMatchHelper(t, "foo/+/#", "fo2/bar/baz")
	noMatchHelper(t, "/#", "foo/bar")
	noMatchHelper(t, "+foo", "+foo")
	noMatchHelper(t, "fo+o", "fo+o")
	noMatchHelper(t, "foo+", "foo+")
	noMatchHelper(t, "+foo/bar", "+foo/bar")
	noMatchHelper(t, "foo+/bar", "foo+/bar")
	noMatchHelper(t, "foo/+bar", "foo/+bar")
	noMatchHelper(t, "foo/bar+", "foo/bar+")
	noMatchHelper(t, "+foo", "afoo")
	noMatchHelper(t, "fo+o", "foao")
	noMatchHelper(t, "foo+", "fooa")
	noMatchHelper(t, "+foo/bar", "afoo/bar")
	noMatchHelper(t, "foo+/bar", "fooa/bar")
	noMatchHelper(t, "foo/+bar", "foo/abar")
	noMatchHelper(t, "foo/bar+", "foo/bara")
	noMatchHelper(t, "#foo", "#foo")
	noMatchHelper(t, "fo#o", "fo#o")
	noMatchHelper(t, "foo#", "foo#")
	noMatchHelper(t, "#foo/bar", "#foo/bar")
	noMatchHelper(t, "foo#/bar", "foo#/bar")
	noMatchHelper(t, "foo/#bar", "foo/#bar")
	noMatchHelper(t, "foo/bar#", "foo/bar#")
	noMatchHelper(t, "foo+", "fooa")
}

func TestTopicUnsubscribe(t *testing.T) {
	topic := "foo/#"
	topicToMatch := "foo/"

	var topicMatcher = NewTopicMatcher()
	if err := topicMatcher.Subscribe(topic, &testSubscriber{value: 10}); err != nil {
		t.Errorf("topic matcher subscribe failed for %s %s\n", topic, topic)
		return
	}

	var subscribers []Subscriber
	if result := topicMatcher.Match(topicToMatch, &subscribers); len(subscribers) != 1 {
		t.Errorf("topic matcher match failed for %s %v %v\n", topic, result, subscribers)
	}

	// now unsubscribe
	err := topicMatcher.Unsubscribe(topic)
	assert.NoError(t, err, "topic matcher unsubscribe failed for %s", topic)
	ts := subscribers[0].(*testSubscriber)
	assert.True(t, ts.finalizeCalled)

	subscribers = subscribers[:0]
	err = topicMatcher.Match(topicToMatch, &subscribers)
	assert.NoError(t, err, "topic matcher Match failed with error")
	assert.Equal(t, 0, len(subscribers), "expected no subscribers but found subscribers %v", subscribers)
}

func TestTopicSubscribers(t *testing.T) {
	subscribeTopics := []string{
		"foo/#",
		"foo/bar/a/+",
		"foo/bar/a/b",
	}
	topicToMatch := "foo/bar/a/b"
	var topicMatcher = NewTopicMatcher()

	i := 0
	for _, v := range subscribeTopics {
		if err := topicMatcher.Subscribe(v, &testSubscriber{value: i}); err != nil {
			return
		}
		i++
	}
	var subscribers []Subscriber
	result := topicMatcher.Match(topicToMatch, &subscribers)
	assert.Equal(t, 3, len(subscribers), "topic matcher match failed for %s %v n subscribers %d\n", topicToMatch, result, len(subscribers))
}
