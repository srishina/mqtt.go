package mqtt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/srishina/mqtt.go/internal/mqttutil"
	"golang.org/x/sync/semaphore"
)

var (
	ErrProtocol = errors.New("Protocol error")
)

type clientOptions struct {
}

var defaultClientOptions = clientOptions{}

// ClientOption contains configurable settings for a client
type ClientOption func(*clientOptions) error

type request struct {
	pkt    packet
	result chan interface{}
	err    error
}

type completionNotifier interface {
	complete(msgID uint16, err error, result interface{})
	completePublishQoS1(pkt *Publish)
	completePublishQoS2(msgID uint16)
}

// MessageHandler callback that is invoked when a new PUBLISH
// message has been received
type MessageHandler func(*Publish)

type messageDispatcher struct {
	recvr   *MessageReceiver
	handler MessageHandler
}

func (m *messageDispatcher) run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		message, err := m.recvr.Recv()
		if err != nil {
			log.Info("Message dispatcher has been closed")
			return
		}
		m.handler(message)
	}
}

type clientSubscription struct {
	subscribe    *Subscribe
	recvr        *MessageReceiver
	cbDispatcher *messageDispatcher
}

type subscriptionCache []*Subscribe

func (sc *subscriptionCache) removeSubscriptionFromCache(topicFilter string) {
	for i, s := range *sc {
		var removed bool
		for j, subscription := range s.Subscriptions {
			if subscription.TopicFilter == topicFilter {
				s.Subscriptions = append(s.Subscriptions[:j], s.Subscriptions[j+1:]...)
				removed = true
				break
			}
		}
		if len(s.Subscriptions) == 0 {
			// Remove the whole subscribe
			*sc = append((*sc)[:i], (*sc)[i+1:]...)
		}
		if removed {
			break
		}
	}
}

func (c *clientSubscription) Finalize() {
	if c.recvr != nil {
		c.recvr.close()
	} else if c.cbDispatcher != nil && c.cbDispatcher.recvr != nil {
		c.cbDispatcher.recvr.close()
	}
}

type packetQueue struct {
	mu     sync.Mutex
	data   []packet
	signal chan struct{}
}

func newPacketQueue() *packetQueue {
	return &packetQueue{signal: make(chan struct{}, 1)}
}

func (p *packetQueue) close() {
	if p.signal != nil {
		close(p.signal)
		p.signal = nil
	}
}

func (p *packetQueue) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.data)
}

// push adds an item to the queue
func (p *packetQueue) push(item packet) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.data = append(p.data, item)
	p.shift()
}

// shift moves the next available item from the queue into the out channel
// the out channel value is returned to the user. must be locked by the caller
func (p *packetQueue) shift() {
	if len(p.data) > 0 {
		select {
		case p.signal <- struct{}{}:
		default:
		}
	}
}

// pop returns the element and the status of the queue (closed or not)
func (p *packetQueue) pop() packet {
	p.mu.Lock()
	defer p.mu.Unlock()
	var item packet
	if len(p.data) > 0 {
		item, p.data = p.data[0], p.data[1:]
	}
	p.shift()
	return item
}

// clientState represents the data that is used to determine the
// pending requests, ongoing/incoming messages and the client topic alias
// mapping. This struct should be used to recover the session state when
// client needs to be reconnected
type clientState struct {
	// as we allow client to publish and (un)subscribe even when not connected
	// we need to synchronize the data
	mu                 sync.Mutex
	connected          bool
	packetsToSend      chan packet
	pendingRequests    map[uint16]*request
	incomingPackets    map[uint16]packet
	outgoingPackets    map[uint16]packet
	clientTopicAliases map[uint16]string
}

type clientStatePacketStore interface {
	storeOutgoing(id uint16, pkt packet)
	storeIncoming(id uint16, pkt packet)
}

func newClientState() *clientState {
	return &clientState{
		pendingRequests:    make(map[uint16]*request),
		incomingPackets:    make(map[uint16]packet),
		outgoingPackets:    make(map[uint16]packet),
		clientTopicAliases: make(map[uint16]string),
		packetsToSend:      make(chan packet, 4),
	}
}

// Client represents a client object
type Client struct {
	conn                 Connection
	options              clientOptions
	mqttConnPkt          *Connect
	store                Store
	pidgenerator         *mqttutil.PIDGenerator
	topicMatcher         *mqttutil.TopicMatcher
	subscriptionCache    subscriptionCache
	incomingPublishQueue *mqttutil.SyncQueue
	state                *clientState
	eventEmitter         *eventEmitter
	wg                   sync.WaitGroup
	notifyOnClose        chan error
	disconnectPkt        chan packet
	stop                 chan struct{}
}

// NewClient creates a new MQTT client
// An MQTT client can be used to perform MQTT operations such
// as connect, publish, subscribe or unsubscribe
func NewClient(conn Connection, opt ...ClientOption) *Client {
	opts := defaultClientOptions

	for _, o := range opt {
		o(&opts)
	}

	return &Client{conn: conn,
		options:              opts,
		store:                newMemStore(),
		pidgenerator:         mqttutil.NewPIDGenerator(),
		topicMatcher:         mqttutil.NewTopicMatcher(),
		incomingPublishQueue: mqttutil.NewSyncQueue(16),
		state:                newClientState(),
		eventEmitter:         newEventEmitter(),
		disconnectPkt:        make(chan packet, 1),
		stop:                 make(chan struct{})}
}

// Connect connect with MQTT broker and send CONNECT MQTT request
func (c *Client) Connect(ctx context.Context, conn *Connect) (*ConnAck, error) {
	c.mqttConnPkt = conn
	var err error
	ph, connAckPkt, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}

	c.eventEmitter.run()

	c.wg.Add(2)
	go c.protocolHandler(ph, connAckPkt)
	go c.messageDispatcher()

	return connAckPkt, nil
}

// Disconnect disconnect from MQTT broker
func (c *Client) Disconnect(ctx context.Context, d *Disconnect) error {
	c.disconnectPkt <- d

	close(c.stop)
	// close the incoming queue
	c.incomingPublishQueue.Close()

	// wait for the goroutines to stop, the reconnector
	c.wg.Wait()

	// close the event emitter, protocol handler is closed now.
	c.eventEmitter.close()

	return nil
}

// Subscribe send MQTT Subscribe request to the broker with the give Subscribe parameter
// and a message channel through which the published messages are returned for the given
// subscribed topics.
// The given topic filters are validated.
// The function waits for the the MQTT SUBSCRIBE response, SubAck, or a packet timeout
// configured as part of the Client options
// Note: the input Subscribe parameter can contain more than one topic, the associated
// MessageReceiver is valid for all the topics present in the given Subscribe.
func (c *Client) Subscribe(ctx context.Context, s *Subscribe, recvr *MessageReceiver) (*SubAck, error) {
	csub := clientSubscription{subscribe: s, recvr: recvr}
	return c.subscribe(ctx, &csub)
}

// CallbackSubscribe send MQTT Subscribe request to the broker with the give Subscribe parameter
// and a callback handler through which the published messages are returned for the given subscribed topics.
// The given topic filters are validated.
// The function waits for the the MQTT SUBSCRIBE response, SubAck, or a packet timeout
// configured as part of the Client options
// Note: the input Subscribe parameter can contain more than one topic, the associated
// Callback handler is valid for all the topics present in the given Subscribe.
func (c *Client) CallbackSubscribe(ctx context.Context, s *Subscribe, cb MessageHandler) (*SubAck, error) {
	recvr := NewMessageReceiver()
	dispatcher := &messageDispatcher{recvr: recvr, handler: cb}
	c.wg.Add(1)
	go dispatcher.run(&c.wg)
	csub := &clientSubscription{subscribe: s, cbDispatcher: dispatcher}
	suback, err := c.subscribe(ctx, csub)
	if err != nil {
		recvr.close()
	}
	return suback, err
}

// On map the callback argument with the event name, more than one
// callback can be added for a particular event name
func (c *Client) On(eventName string, callback interface{}) error {
	return c.eventEmitter.on(eventName, callback)
}

// Off removed the callback associated with the event name
func (c *Client) Off(eventName string, value interface{}) error {
	return c.eventEmitter.emit(eventName, value)
}

func (c *Client) subscribe(ctx context.Context, s *clientSubscription) (*SubAck, error) {
	// add to topic Matcher
	for _, subscription := range s.subscribe.Subscriptions {
		if err := c.topicMatcher.Subscribe(subscription.TopicFilter, s); err != nil {
			log.Errorf("Unable to subscribe the topic %v", err)
			return nil, err
		}
	}
	return c.subscribeInternal(ctx, s.subscribe)
}

func (c *Client) subscribeInternal(ctx context.Context, s *Subscribe) (*SubAck, error) {
	s.packetID = c.pidgenerator.NextID()

	req := &request{pkt: s, result: make(chan interface{})}
	// add it to ongoing requests
	c.state.mu.Lock()
	c.state.pendingRequests[s.packetID] = req
	// push to the outgoing queue
	c.state.outgoingPackets[s.packetID] = req.pkt
	if c.state.connected {
		c.state.packetsToSend <- req.pkt
	}
	c.state.mu.Unlock()

	var result interface{}
	select {
	case <-ctx.Done():
		fmt.Println("Error waiting for SUBACK ", ctx.Err())
		return nil, ctx.Err()
	case result = <-req.result:
	}

	if suback, ok := result.(*SubAck); ok {
		// store the client subscription in the cache (used while resubscribing)
		c.subscriptionCache = append(c.subscriptionCache, s)
		return suback, nil
	}

	return nil, fmt.Errorf("Internal error during SUBSCRIBE, invalid typs received")
}

func (c *Client) resubscribe(ctx context.Context, s *Subscribe) (*SubAck, error) {
	s.packetID = c.pidgenerator.NextID()

	req := &request{pkt: s, result: make(chan interface{})}
	// add it to ongoing requests

	c.state.mu.Lock()
	c.state.pendingRequests[s.packetID] = req
	// push to the outgoing queue
	c.state.outgoingPackets[s.packetID] = req.pkt
	if c.state.connected {
		c.state.packetsToSend <- req.pkt
	}
	c.state.mu.Unlock()

	var result interface{}
	select {
	case <-ctx.Done():
		fmt.Println("Error waiting for SUBACK ", ctx.Err())
		return nil, ctx.Err()
	case result = <-req.result:
	}

	if suback, ok := result.(*SubAck); ok {
		return suback, nil
	}

	return nil, fmt.Errorf("Internal error during SUBSCRIBE, invalid typs received")
}

// Unsubscribe send MQTT UNSUBSCRIBE request to the broker with the give Unsubscribe parameter
// The function waits for the the MQTT SUBSCRIBE response, SubAck, or for a packet timeout
func (c *Client) Unsubscribe(ctx context.Context, us *Unsubscribe) (*UnsubAck, error) {
	for _, topicFilter := range us.TopicFilters {
		if err := c.topicMatcher.Unsubscribe(topicFilter); err != nil {
			return nil, err
		}
		// remove from subscription cache
		c.subscriptionCache.removeSubscriptionFromCache(topicFilter)
	}

	us.packetID = c.pidgenerator.NextID()

	req := &request{pkt: us, result: make(chan interface{})}
	// add it to ongoing requests
	c.state.mu.Lock()
	c.state.pendingRequests[us.packetID] = req
	// push to the outgoing queue
	c.state.outgoingPackets[us.packetID] = req.pkt
	if c.state.connected {
		c.state.packetsToSend <- req.pkt
	}
	c.state.mu.Unlock()

	var result interface{}

	select {
	case <-ctx.Done():
		fmt.Println("Error waiting for UNSUBACK ", ctx.Err())
		return nil, ctx.Err()
	case result = <-req.result:
	}

	if unsuback, ok := result.(*UnsubAck); ok {
		return unsuback, req.err
	}

	return nil, fmt.Errorf("Internal error during UNSUBSCRIBE, invalid typs received")
}

// Publish send MQTT PUBLISH packet to the MQTT broker. When the QoS is 1 or 2
// the function waits for a response from the broker and for QoS 0 the function
// complets immediatly after the PUBLISH message is schedule to send.
func (c *Client) Publish(ctx context.Context, p *Publish) error {
	if err := mqttutil.ValidatePublishTopic(p.TopicName); err != nil {
		return err
	}

	mapTopicAlias := func() {
		if p.Properties.TopicAlias != nil && len(p.TopicName) > 0 {
			c.state.mu.Lock()
			// check if topic alias has been set
			c.state.clientTopicAliases[*p.Properties.TopicAlias] = p.TopicName
			c.state.mu.Unlock()
		} else if len(p.TopicName) > 0 && p.Properties.TopicAlias == nil {
			// delete topic alias if the client did a reset
			c.state.mu.Lock()
			for k, v := range c.state.clientTopicAliases {
				if v == p.TopicName {
					delete(c.state.clientTopicAliases, k)
					break
				}
			}
			c.state.mu.Unlock()
		}
	}

	if p.QoSLevel > 0 {
		p.packetID = c.pidgenerator.NextID()

		req := &request{pkt: p, result: make(chan interface{})}
		// add it to ongoing requests
		c.state.mu.Lock()
		c.state.pendingRequests[p.packetID] = req
		// push to the outgoing queue
		c.state.outgoingPackets[p.packetID] = req.pkt
		if c.state.connected {
			c.state.packetsToSend <- req.pkt
		}
		c.state.mu.Unlock()

		select {
		case <-ctx.Done():
			fmt.Println("timeout waiting for PUBLISH ", ctx.Err())
			return ctx.Err()
		case _ = <-req.result:
			mapTopicAlias()
		}
		return nil
	}

	// send QoS0, will be discarded if there is no connection
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.connected {
		c.state.packetsToSend <- p
		mapTopicAlias()
		return nil
	}

	return fmt.Errorf("Disconnected - QoS0 packets will be discarded")
}

func (c *Client) messageDispatcher() error {
	defer c.wg.Done()
	for {
		closed, item := c.incomingPublishQueue.Pop()
		if closed {
			break
		}

		if msg, ok := item.(*Publish); ok {
			var subscribers []mqttutil.Subscriber
			if err := c.topicMatcher.Match(msg.TopicName, &subscribers); err != nil {
				log.Warnf("Topic (%s) matching failed with error %v", msg.TopicName, err)
			}
			if len(subscribers) != 0 {
				if notifier, ok := subscribers[0].(*clientSubscription); ok {
					if notifier.recvr != nil {
						notifier.recvr.send(msg)
					} else if notifier.cbDispatcher != nil {
						notifier.cbDispatcher.recvr.send(msg)
					} else {
						log.Warn("Message received, but no dispatcher available")
					}
				}
			}
		}
	}
	return nil
}

func (c *Client) connect(ctx context.Context) (*protocolHandler, *ConnAck, error) {
	// dial and wait for a connection to succeed or error
	rw, err := c.conn.Connect(ctx)
	if err != nil {
		return nil, nil, err
	}

	ph := protocolHandler{options: &c.options,
		rw:                        rw,
		packetsToSend:             newPacketQueue(),
		qos12PublishPacketSlotAvb: make(chan struct{}, 1),
		stop:                      make(chan struct{}),
	}

	var wg sync.WaitGroup
	done := make(chan struct{}, 1)
	defer close(done)
	var connAckPkt *ConnAck
	wg.Add(1)
	go func() {
		defer wg.Done()
		connAckPkt, err = ph.connect(c.mqttConnPkt)
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		fmt.Println("cancelled connect due to timeout")
		// close the connection will force connect call to return
		c.conn.Close()
	}
	wg.Wait()

	if err != nil {
		return nil, nil, err
	}

	// signal when operation like Publish, Subscribe, Unsubscribe completes
	ph.notifer = c
	ph.incomingPublishQ = c.incomingPublishQueue
	ph.packetStore = c

	c.notifyOnClose = make(chan error, 1)
	ph.notifyClose(c.notifyOnClose)

	return &ph, connAckPkt, err
}

// must be called from state locked context
func (c *Client) restoreState(ph *protocolHandler, connack *ConnAck, sem *semaphore.Weighted) []packet {
	pendingQoS12Pkts := []packet{}
	// and drain the packetsToSend channel
	for {
		var stop bool
		select {
		case <-c.state.packetsToSend:
		default:
			stop = true
		}
		if stop {
			break
		}
	}

	if !connack.SessionPresent {
		cache := c.subscriptionCache
		c.subscriptionCache = subscriptionCache{}
		// resubscribe
		for _, s := range cache {
			c.wg.Add(1)
			go func(s *Subscribe) {
				defer c.wg.Done()
				suback, err := c.resubscribe(context.Background(), s)
				if err != nil {
					c.eventEmitter.emit(ResubscribeEvent, ResubscribeResult{Subscribe: s, Error: err})
				}
				c.eventEmitter.emit(ResubscribeEvent, ResubscribeResult{Subscribe: s, SubAck: suback})
			}(s)
		}
		pendingReqs := c.state.pendingRequests
		c.state.incomingPackets = make(map[uint16]packet)
		c.state.outgoingPackets = make(map[uint16]packet)
		c.state.pendingRequests = make(map[uint16]*request)

		// now we need to send all pending requests
		for _, req := range pendingReqs {
			id := c.pidgenerator.NextID()
			switch req.pkt.(type) {
			case *Publish:
				req.pkt.(*Publish).packetID = id
			case *Subscribe:
				req.pkt.(*Subscribe).packetID = id
			case *Unsubscribe:
				req.pkt.(*Unsubscribe).packetID = id
			default:
				fmt.Println("Invalid packet found in pending requests")
				c.pidgenerator.FreeID(id)
				continue
			}
			c.state.outgoingPackets[id] = req.pkt
			c.state.pendingRequests[id] = req
		}
	}

	mappedAliases := make(map[string]uint16)
	// schedule all packets
	for _, pkt := range c.state.outgoingPackets {
		publishPkt, ok := pkt.(*Publish)
		if ok {
			// add check for topic alias
			if publishPkt.Properties.TopicAlias != nil && len(publishPkt.TopicName) == 0 {
				// check whether we have a topic associated
				if v, ok := c.state.clientTopicAliases[*publishPkt.Properties.TopicAlias]; ok {
					// check if it is alread set
					if _, ok := mappedAliases[v]; !ok {
						publishPkt.TopicName = v
						mappedAliases[v] = *publishPkt.Properties.TopicAlias
					}
				}
			}
			publishPkt.DUPFlag = true
			if publishPkt.QoSLevel > 0 && !sem.TryAcquire(1) {
				// we cant schedule now, push to pending publish QoS12 queue
				pendingQoS12Pkts = append(pendingQoS12Pkts, publishPkt)
				continue
			}
		}
		ph.schedule(pkt)
	}

	return pendingQoS12Pkts
}

func (c *Client) protocolHandler(ph *protocolHandler, connAckPkt *ConnAck) {
	defer c.wg.Done()

	var semServerRecvMax *semaphore.Weighted
	onconnected := func() []packet {
		defaultRecvMax := 65535
		if connAckPkt.Properties.ReceiveMaximum != nil {
			defaultRecvMax = int(*(connAckPkt.Properties.ReceiveMaximum))
		}
		// if the session is present then we should preserve the state,
		// i.e to know how many packets are sent
		if semServerRecvMax == nil || !connAckPkt.SessionPresent {
			semServerRecvMax = semaphore.NewWeighted(int64(defaultRecvMax))
		}

		c.state.mu.Lock()
		pendingQoS12Pkts := c.restoreState(ph, connAckPkt, semServerRecvMax)
		c.state.connected = true
		c.state.mu.Unlock()

		// run the protocol handler
		ph.semServerRecvMax = semServerRecvMax
		ph.run()
		return pendingQoS12Pkts
	}

	pendingQoS12PublishPackets := onconnected()

	continuation := make(chan struct{}, 1)
	for {
		var stopping bool
		var err error
		var d packet
		select {
		case d = <-c.disconnectPkt:
			select {
			case <-c.stop:
				stopping = true
			default:
			}
		case <-c.stop:
			select {
			case d = <-c.disconnectPkt:
			default:
			}
			stopping = true
		case err = <-c.notifyOnClose:
		case <-ph.qos12PublishPacketSlotAvb:
			i := 0
			for j, pkt := range pendingQoS12PublishPackets {
				if ph.semServerRecvMax.TryAcquire(1) {
					// We now acquired a semaphore, schedule the packet
					ph.schedule(pkt)
					pendingQoS12PublishPackets[j] = nil // so that the items are garbage collected
				} else {
					pendingQoS12PublishPackets[i] = pkt // shift
					i++
				}
			}
			pendingQoS12PublishPackets = pendingQoS12PublishPackets[:i]
			continue
		case pkt := <-c.state.packetsToSend:
			if publishPkt, ok := pkt.(*Publish); ok {
				if publishPkt.QoSLevel > 0 && !ph.semServerRecvMax.TryAcquire(1) {
					// we cant schedule now, push to pending publish QoS12 queue
					pendingQoS12PublishPackets = append(pendingQoS12PublishPackets, pkt)
					continue
				}
			}
			ph.schedule(pkt)
			continue
		case <-continuation:
		}

		if d != nil {
			ph.sendPacket(d)
		}

		// set disconncted, from this point onwards we will not receive data in packetsToSend channel
		// when (and if) reconnected we will drain the pending request channel based on the session state
		// in the server
		c.state.mu.Lock()
		c.state.connected = false
		c.state.mu.Unlock()

		c.conn.Close() // close the underlying connection

		// wait for PH goroutines to stop
		ph.waitForCompletion()

		if stopping {
			return
		}

		pendingQoS12PublishPackets = pendingQoS12PublishPackets[:0]

		// emit the disconnected event
		c.eventEmitter.emit(DisconnectedEvent, err)

		// try to connect after 500secs
		time.Sleep(500 * time.Millisecond)

		// inform that we are connecting
		c.eventEmitter.emit(ReconnectingEvent, "Trying to connect")
		reconnectCtx, reconnectCanceFn := context.WithCancel(context.Background())
		reconnected := make(chan struct{})
		var connack *ConnAck
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			ph, connack, err = c.connect(reconnectCtx)
			pendingQoS12PublishPackets = onconnected()
			close(reconnected)
		}()
		select {
		case <-reconnectCtx.Done():
			// we are reconnected or has an error
			if err != nil {
				c.eventEmitter.emit(DisconnectedEvent, err)
			}
		case <-reconnected:
		case <-c.stop:
			// we are closing, cancel the reconnect
			stopping = true
		}
		reconnectCanceFn()
		if stopping {
			return
		}

		if err != nil {
			continuation <- struct{}{}
			continue

		}
		// inform that we are reconnected
		c.eventEmitter.emit(ReconnectedEvent, connack)
	}
}

func (c *Client) complete(msgID uint16, err error, result interface{}) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if req, ok := c.state.pendingRequests[msgID]; ok {
		req.err = err
		req.result <- result

		close(req.result)

		// Delete the pending request from queue
		delete(c.state.pendingRequests, msgID)
		if _, ok := c.state.outgoingPackets[msgID]; !ok {
			fmt.Println("outgoing packet not found")
		}
		// remove from outgoing queue
		delete(c.state.outgoingPackets, msgID)
	} else {
		fmt.Println("pending request not found")
	}

	c.pidgenerator.FreeID(msgID)
}

func (c *Client) storeOutgoing(id uint16, pkt packet) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	// push to the outgoing queue
	c.state.outgoingPackets[id] = pkt
}

func (c *Client) storeIncoming(id uint16, pkt packet) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.incomingPackets[id] = pkt
}

func (c *Client) completePublishQoS1(pkt *Publish) {
	c.incomingPublishQueue.Push(pkt)
}

func (c *Client) completePublishQoS2(id uint16) {
	c.state.mu.Lock()
	if pkt, ok := c.state.incomingPackets[id]; ok {
		if publishPkt, ok := pkt.(*Publish); ok {
			c.incomingPublishQueue.Push(publishPkt)
		} else {
			log.Warnf("invalid packet in storage for incoming packet id %d", id)
		}
		// delete the publish packet from the store
		delete(c.state.incomingPackets, id)
		// remove the PUBREC packet outgoing queue, received as part of PUBLISH
		// add a check
		if pkt, ok := c.state.outgoingPackets[id]; ok {
			if _, ok := pkt.(*PubRec); ok {
				delete(c.state.outgoingPackets, id)
			} else {
				log.Warnf("Invalid packet found in storage, supposed to be PUBREC, id: %d", id)
			}
		} else {
			log.Warnf("PUBREC packet is not found in the storage id: %d", id)
		}

	} else {
		log.Warnf("Publish packet is not found in the storage id: %d", id)
	}
	c.state.mu.Unlock()
}

type protocolHandler struct {
	options                   *clientOptions
	wg                        sync.WaitGroup
	rw                        io.ReadWriter
	packetStore               clientStatePacketStore
	packetsToSend             *packetQueue
	notifer                   completionNotifier
	incomingPublishQ          *mqttutil.SyncQueue
	keepAliveTimeout          time.Duration
	keepAliveTicker           *time.Ticker
	pingRespRecvd             chan struct{}
	errWhenClosed             chan<- error
	semServerRecvMax          *semaphore.Weighted
	qos12PublishPacketSlotAvb chan struct{}
	stopCloseOnce             sync.Once
	stop                      chan struct{}
	mu                        sync.Mutex
}

func (c *protocolHandler) connect(mqttConnect *Connect) (*ConnAck, error) {
	if err := c.sendPacket(mqttConnect); err != nil {
		return nil, err
	}
	// receive the connack
	pkt, err := readFrom(c.rw)
	connAckPkt, ok := pkt.(*ConnAck)
	if !ok {
		c.disconnect(&Disconnect{ReasonCode: DisconnectReasonCodeProtocolError})
		return nil, errors.New("invalid response received for CONNECT request")
	}

	// if there is keep alive timeout in CONNACK use that value as timeout
	if connAckPkt.Properties.ServerKeepAlive != nil {
		c.keepAliveTimeout = time.Duration(*connAckPkt.Properties.ServerKeepAlive) * time.Second
	} else {
		c.keepAliveTimeout = time.Duration(mqttConnect.KeepAlive) * time.Second
	}

	return connAckPkt, err
}

func (c *protocolHandler) disconnect(d *Disconnect) error {
	return c.sendPacket(d)
}

func (c *protocolHandler) shutdown(e error) {
	c.stopCloseOnce.Do(func() {
		close(c.stop)
		if e != nil && c.errWhenClosed != nil {
			c.errWhenClosed <- e
		}
		c.errWhenClosed = nil
	})
}

func (c *protocolHandler) notifyClose(e chan<- error) {
	c.errWhenClosed = e
}

func (c *protocolHandler) run() {
	c.wg.Add(2)
	go c.receiver()
	go c.sender()
	// start keep alive timer
	if c.keepAliveTimeout > 0 {
		c.wg.Add(1)
		c.pingRespRecvd = make(chan struct{}, 1)
		c.keepAliveTicker = time.NewTicker(c.keepAliveTimeout)
		go c.pinger()
	}
}

func (c *protocolHandler) schedule(pkt packet) {
	// push to the outgoing queue
	c.packetsToSend.push(pkt)
}

func (c *protocolHandler) waitForCompletion() {
	c.shutdown(nil)
	c.wg.Wait()

	if c.pingRespRecvd != nil {
		close(c.pingRespRecvd)
	}

	if c.keepAliveTicker != nil {
		c.keepAliveTicker.Stop()
	}
}

func (c *protocolHandler) receiver() {
	defer c.wg.Done()
	for {
		pkt, err := readFrom(c.rw)
		if err == nil {
			// we now received a packet, reset the keep alive timer
			c.resetKeepAliveTimer()
			err = c.process(pkt)
		}

		if err != nil {
			// close the connection
			c.shutdown(err)
			return
		}
	}
}

func (c *protocolHandler) sender() {
	defer c.wg.Done()
	for {
		select {
		case _, ok := <-c.packetsToSend.signal:
			if ok {
				pkt := c.packetsToSend.pop()
				if err := c.sendPacket(pkt); err != nil {
					log.Error("Send packet returned with error ", err)
					c.shutdown(err)
					return
				}
			}
		case <-c.stop:
			return
		}
	}
}

func (c *protocolHandler) resetKeepAliveTimer() {
	if c.keepAliveTimeout > 0 {
		c.keepAliveTicker.Reset(c.keepAliveTimeout)
	}
}

func (c *protocolHandler) pinger() {
	var waitForPingResp bool
	defer c.wg.Done()
	for {
		select {
		case <-c.keepAliveTicker.C:
			if waitForPingResp {
				// send disconnect and shutdown
				c.disconnect(&Disconnect{ReasonCode: DisconnectReasonCodeKeepAliveTimeout})
				c.shutdown(errors.New("pingresp not received, disconnecting"))
				return
			}

			c.sendPacket(&pingReq{})
			waitForPingResp = true
			c.resetKeepAliveTimer()

		case <-c.pingRespRecvd:
			waitForPingResp = false
		case <-c.stop:
			c.keepAliveTicker.Stop()
			return
		}
	}
}

func (c *protocolHandler) process(pkt packet) error {
	switch pkt.(type) {
	case *Publish:
		return c.publishHandler(pkt.(*Publish))
	case *PubAck:
		return c.pubAckHandler(pkt.(*PubAck))
	case *PubRec:
		return c.pubRecHandler(pkt.(*PubRec))
	case *PubRel:
		return c.pubRelHandler(pkt.(*PubRel))
	case *PubComp:
		return c.pubCompHandler(pkt.(*PubComp))
	case *SubAck:
		return c.subAckHandler(pkt.(*SubAck))
	case *UnsubAck:
		return c.unSubAckHandler(pkt.(*UnsubAck))
	case *pingResp:
		return c.pingRespHandler(pkt.(*pingResp))
	default:
		// unrecognized message
		return ErrProtocol
	}
}

func (c *protocolHandler) publishHandler(msg *Publish) error {
	// notify the message to the subscribed client, for QoS 0, 1
	// for QoS 2 the notification happens when we send PubComp message

	switch msg.QoSLevel {
	case 0:
		c.incomingPublishQ.Push(msg)
	case 1:
		pubAckPacket := &PubAck{}
		pubAckPacket.ReasonCode = PubAckReasonCodeSuccess
		pubAckPacket.packetID = msg.packetID
		c.schedule(pubAckPacket)

		// get the publish packet and push the packet into incommng message queue
		// for consumption
		c.notifer.completePublishQoS1(msg)
	case 2:
		// send a pub rec message
		pubRecPacket := &PubRec{}
		pubRecPacket.ReasonCode = PubRecReasonCode(PubRelReasonCodeSuccess)
		pubRecPacket.packetID = msg.packetID

		// store the publish packet in inbound queue, once we receive the
		// pubrel we wiill push the packet into incoming message queue for
		// consumption
		c.packetStore.storeIncoming(msg.packetID, msg)

		// store the pub rec in outgoing
		c.packetStore.storeOutgoing(msg.packetID, pubRecPacket)
		c.schedule(pubRecPacket)
	}
	return nil
}

func (c *protocolHandler) outgoingPublishCompleted() {
	// release a semaphore
	c.semServerRecvMax.Release(1)

	// signal that we now have a new slot for publish with QoS 1 & 2 packet
	// and if it is already signalled then we can keep going, protocolHandler will
	// schedule the packets for the number of free slots
	select {
	case c.qos12PublishPacketSlotAvb <- struct{}{}:
	default:
	}
}

func (c *protocolHandler) pubAckHandler(pkt *PubAck) error {
	c.notifer.complete(pkt.packetID, nil, nil)
	c.outgoingPublishCompleted()
	return nil
}

func (c *protocolHandler) pubRecHandler(pubrec *PubRec) error {
	pubRelPacket := &PubRel{}
	pubRelPacket.ReasonCode = PubRelReasonCodeSuccess
	pubRelPacket.packetID = pubrec.packetID

	// store the pub rec in outgoing, will overwrite the PUBLISH packet
	c.packetStore.storeOutgoing(pubRelPacket.packetID, pubRelPacket)
	c.schedule(pubRelPacket)
	return nil
}

func (c *protocolHandler) pubRelHandler(pubrel *PubRel) error {
	pubCompPacket := &PubComp{}
	pubCompPacket.ReasonCode = PubCompReasonCodeSuccess
	pubCompPacket.packetID = pubrel.packetID

	// get the publish packet and push the packet into incoming message queue
	// for consumption
	c.notifer.completePublishQoS2(pubrel.packetID)

	c.schedule(pubCompPacket)
	return nil
}

func (c *protocolHandler) pubCompHandler(pubcomp *PubComp) error {
	c.notifer.complete(pubcomp.packetID, nil, nil)
	c.outgoingPublishCompleted()
	return nil
}

func (c *protocolHandler) subAckHandler(suback *SubAck) error {
	c.notifer.complete(suback.packetID, nil, suback)
	return nil
}

func (c *protocolHandler) unSubAckHandler(unsuback *UnsubAck) error {
	c.notifer.complete(unsuback.packetID, nil, unsuback)
	return nil
}

func (c *protocolHandler) pingRespHandler(pingresp *pingResp) error {
	c.pingRespRecvd <- struct{}{}
	return nil
}

func (c *protocolHandler) sendPacket(pkt packet) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	writer := mqttutil.NewBufioWriterSize(c.rw, 2*1024)
	defer mqttutil.PutBufioWriter(writer)
	if err := writeTo(pkt, writer); err != nil {
		return err
	}
	return writer.Flush()
}
