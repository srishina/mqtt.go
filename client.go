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
)

var (
	ErrProtocol = errors.New("Protocol error")
)

// clientOptions contains configurable settings for a client
type clientOptions struct {
	connectionTimeout time.Duration
	packetTimeout     time.Duration
}

var defaultClientOptions = clientOptions{
	connectionTimeout: 15 * time.Second,
	packetTimeout:     10 * time.Second,
}

// ClientOption ...
type ClientOption func(*clientOptions) error

// WithConnectionTimeout duration for the transaction of single MQTT packet
func WithConnectionTimeout(timeout time.Duration) ClientOption {
	return func(c *clientOptions) error {
		c.connectionTimeout = timeout
		return nil
	}
}

// WithPacketTimeout duration for the transaction of single MQTT packet
func WithPacketTimeout(timeout time.Duration) ClientOption {
	return func(c *clientOptions) error {
		c.packetTimeout = timeout
		return nil
	}
}

type request struct {
	result chan interface{}
	err    error
}

type ongoingRequests struct {
	ongoingRequestsMtx sync.RWMutex
	ongoingRequests    map[uint16]*request
}

func newOngoingRequests() *ongoingRequests {
	return &ongoingRequests{
		ongoingRequests: make(map[uint16]*request),
	}
}

func (o *ongoingRequests) add(id uint16, r *request) error {
	o.ongoingRequestsMtx.Lock()
	defer o.ongoingRequestsMtx.Unlock()
	if _, ok := o.ongoingRequests[id]; ok {
		return fmt.Errorf("Error: Trying to use a packet ID that is already in use")
	}
	o.ongoingRequests[id] = r
	return nil
}

func (o *ongoingRequests) get(id uint16) (*request, error) {
	o.ongoingRequestsMtx.Lock()
	defer o.ongoingRequestsMtx.Unlock()
	if r, ok := o.ongoingRequests[id]; ok {
		return r, nil
	}
	return nil, fmt.Errorf("Error: Packet ID %d does not exist, unable to retrieve", id)
}

func (o *ongoingRequests) remove(id uint16) error {
	o.ongoingRequestsMtx.Lock()
	defer o.ongoingRequestsMtx.Unlock()
	delete(o.ongoingRequests, id)
	return nil
}

type completionNotifier interface {
	complete(msgID uint16, err error, result interface{})
}

// Message to be deliverd to client, currently we deliver the message
// and the associated topic
type Message interface {
	Topic() string
	Payload() []byte
}

type message struct {
	topic   string
	payload []byte
}

func (m *message) Topic() string {
	return m.topic
}

func (m *message) Payload() []byte {
	return m.payload
}

// MessageHandler callback that is invoked when a new PUBLISH
// message has been received
type MessageHandler func(Message)

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
	recvr        *MessageReceiver
	cbDispatcher *messageDispatcher
}

func (c *clientSubscription) Finalize() {
	if c.recvr != nil {
		c.recvr.close()
	} else if c.cbDispatcher != nil && c.cbDispatcher.recvr != nil {
		c.cbDispatcher.recvr.close()
	}
}

const (
	inbound  = 1 << iota
	outbound = 1 << iota
)

func merge(pid uint16, dir uint16) uint32 {
	return (uint32(dir) << 16) + uint32(pid)
}

func split(key uint32) (uint16, uint16) {
	return uint16(key >> 16), uint16(key & 0x0000FFFF)
}

func storeInBound(store Store, pid uint16, pkt packet) {
	key := merge(pid, inbound)
	store.Insert(key, pkt)
}

func storeOutbound(store Store, pid uint16, pkt packet) {
	key := merge(pid, outbound)
	store.Insert(key, pkt)
}

func fetchInbound(store Store, pid uint16) packet {
	key := merge(pid, inbound)
	return store.GetByID(key)
}

func fetchOutbound(store Store, pid uint16) packet {
	key := merge(pid, outbound)
	return store.GetByID(key)
}

func deleteInbound(store Store, pid uint16) {
	key := merge(pid, inbound)
	store.DeleteByID(key)
}

func deleteOutbound(store Store, pid uint16) {
	key := merge(pid, outbound)
	store.DeleteByID(key)
}

// Client ...
type Client struct {
	conn             Connection
	options          clientOptions
	ph               *protocolHandler
	store            Store
	pidgenerator     *mqttutil.PIDGenerator
	topicMatcher     *mqttutil.TopicMatcher
	incomingMsgQueue *mqttutil.SyncQueue
	ongoingRequests  *ongoingRequests
	wg               sync.WaitGroup
	notifyOnClose    chan error
	stop             chan struct{}
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
		options:          opts,
		store:            newMemStore(),
		pidgenerator:     mqttutil.NewPIDGenerator(),
		topicMatcher:     mqttutil.NewTopicMatcher(),
		incomingMsgQueue: mqttutil.NewSyncQueue(16),
		ongoingRequests:  newOngoingRequests(),
		stop:             make(chan struct{})}
}

// Connect connect with MQTT broker and send CONNECT MQTT request
func (c *Client) Connect(ctx context.Context, conn *Connect) (*ConnAck, error) {
	var err error
	connAckPkt, err := c.connect(ctx, &c.options, c, c.incomingMsgQueue, c.store, conn)
	if err != nil {
		return nil, err
	}

	c.wg.Add(2)
	go c.reconnector()
	go c.messageDispatcher()

	return connAckPkt, nil
}

// Disconnect disconnect from MQTT broker
func (c *Client) Disconnect(ctx context.Context, d *Disconnect) error {
	err := c.ph.disconnect(d)
	if err != nil {
		fmt.Println("Sending protocol disconnect returned error: ", err)
	}

	close(c.stop)
	// close the incoming queue
	c.incomingMsgQueue.Close()

	// wait for the goroutines to stop, the reconnector
	c.wg.Wait()

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
	csub := clientSubscription{recvr: recvr}
	return c.subscribe(ctx, s, &csub)
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
	csub := &clientSubscription{cbDispatcher: dispatcher}
	suback, err := c.subscribe(ctx, s, csub)
	if err != nil {
		recvr.close()
	}
	return suback, err
}

func (c *Client) subscribe(ctx context.Context, s *Subscribe, csub mqttutil.Subscriber) (*SubAck, error) {
	// add to topic Matcher
	for _, subscription := range s.Subscriptions {
		if err := c.topicMatcher.Subscribe(subscription.TopicFilter, csub); err != nil {
			log.Errorf("Unable to subscribe the topic %v", err)
			return nil, err
		}
	}

	s.packetID = c.pidgenerator.NextID()
	// store the packet
	storeOutbound(c.store, s.packetID, s)

	req := &request{result: make(chan interface{})}
	// add it to ongoing requests
	if err := c.ongoingRequests.add(s.packetID, req); err != nil {
		return nil, err
	}

	c.ph.outbound <- s

	var result interface{}
	subCtx, cf := context.WithTimeout(ctx, c.options.packetTimeout)
	defer cf()

	select {
	case <-subCtx.Done():
		if e := subCtx.Err(); e == context.DeadlineExceeded {
			fmt.Println("timeout waiting for SUBACK")
			c.ongoingRequests.remove(s.packetID)
			return nil, e
		}
	case result = <-req.result:
	}

	if suback, ok := result.(*SubAck); ok {
		c.ongoingRequests.remove(s.packetID)
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
	}

	us.packetID = c.pidgenerator.NextID()

	// store the packet
	storeOutbound(c.store, us.packetID, us)

	req := &request{result: make(chan interface{})}
	// add it to ongoing requests
	if err := c.ongoingRequests.add(us.packetID, req); err != nil {
		return nil, err
	}

	c.ph.outbound <- us

	var result interface{}
	subCtx, cf := context.WithTimeout(ctx, c.options.packetTimeout)
	defer cf()

	select {
	case <-subCtx.Done():
		if e := subCtx.Err(); e == context.DeadlineExceeded {
			fmt.Println("timeout waiting for UNSUBACK")
			return nil, e
		}
	case result = <-req.result:
	}

	if unsuback, ok := result.(*UnsubAck); ok {
		c.ongoingRequests.remove(us.packetID)
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

	if p.QoSLevel > 0 {
		p.packetID = c.pidgenerator.NextID()
		// store the packet
		storeOutbound(c.store, p.packetID, p)

		req := &request{result: make(chan interface{})}
		// add it to ongoing requests
		if err := c.ongoingRequests.add(p.packetID, req); err != nil {
			return err
		}
		c.ph.outbound <- p

		pubCtx, cf := context.WithTimeout(ctx, c.options.packetTimeout)
		defer cf()

		select {
		case <-pubCtx.Done():
			if e := pubCtx.Err(); e == context.DeadlineExceeded {
				fmt.Println("timeout waiting for UNSUBACK")
				return e
			}
		case _ = <-req.result:
		}
		return nil
	}

	return c.ph.sendPacket(p)
}

func (c *Client) complete(msgID uint16, err error, result interface{}) {
	if req, err2 := c.ongoingRequests.get(msgID); err2 == nil {
		req.err = err
		req.result <- result

		close(req.result)
		// the message is completed, remove from the store
		deleteOutbound(c.store, msgID)
	}
}

func (c *Client) messageDispatcher() error {
	defer c.wg.Done()
	for {
		closed, item := c.incomingMsgQueue.Pop()
		if closed {
			break
		}

		if msg, ok := item.(Message); ok {
			var subscribers []mqttutil.Subscriber
			if err := c.topicMatcher.Match(msg.Topic(), &subscribers); err != nil {
				log.Warnf("Topic (%s) matching failed with error %v", msg.Topic(), err)
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

// func dial(ctx context.Context, url *url.URL) (net.Conn, error) {
// 	var err error
// 	var conn net.Conn
// 	switch url.Scheme {
// 	case "ws":
// 		conn, err = mqttutil.NewWebSocket(ctx, url.String(), nil, http.Header{})
// 	case "wss":
// 		conn, err = mqttutil.NewWebSocket(ctx, url.String(), nil, http.Header{})
// 	case "tcp":
// 		dialer := net.Dialer{}
// 		conn, err = dialer.DialContext(ctx, "tcp", url.Host)
// 	case "tcps":
// 		// todo...
// 	}

// 	return conn, err
// }

func (c *Client) connect(ctx context.Context,
	opt *clientOptions, notifer completionNotifier, incomingQ *mqttutil.SyncQueue, store Store, mqttConnect *Connect) (*ConnAck, error) {
	connCtx, connCancelFn := context.WithTimeout(ctx, c.options.connectionTimeout)
	defer connCancelFn()

	// dial and wait for a connection to succeed or error
	rw, err := c.conn.Connect((connCtx))
	if err != nil {
		return nil, err
	}

	ph := protocolHandler{options: opt,
		rw:       rw,
		outbound: make(chan packet, 4),
		stop:     make(chan struct{}),
	}

	phConnCtx, phConnCancelFn := context.WithTimeout(ctx, c.options.packetTimeout)
	defer phConnCancelFn()

	var wg sync.WaitGroup
	done := make(chan struct{}, 1)
	defer close(done)
	var connAckPkt *ConnAck
	wg.Add(1)
	go func() {
		defer wg.Done()
		connAckPkt, err = ph.connect(mqttConnect)
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-phConnCtx.Done():
		fmt.Println("cancelled connect due to timeout")
		// close the connection will force connect call to return
		c.conn.Close()
	}
	wg.Wait()

	if err != nil {
		return nil, err
	}

	// signal when operation like Publish, Subscribe, Unsubscribe completes
	ph.notifer = notifer
	ph.incomingMsgQ = incomingQ
	ph.store = store

	c.ph = &ph
	c.notifyOnClose = make(chan error, 1)
	ph.notifyClose(c.notifyOnClose)
	ph.run()

	return connAckPkt, err
}

func (c *Client) reconnector() {
	defer c.wg.Done()

	// maxRetry := 0
	// retried := 0

	var err error
	for {
		var stopping bool
		select {
		case <-c.stop:
			stopping = true
		case err = <-c.notifyOnClose:
		}

		c.conn.Close() // close the underlying connection
		c.conn = nil
		// wait for PH goroutines to stop
		c.ph.waitForCompletion()

		if stopping {
			return
		}

		log.Info("Client is disconnected with error ", err)

		// retried++
		// // try to connect, we connect 3 more times todo: make it configurable with retrying intervals
		// if err := c.connect(context.Background(), &c.url, &c.options); err == nil {
		// 	retried = 0
		// 	continue
		// }

		// if retried < maxRetry {
		// 	fmt.Println("Connecting to mqtt server is failed, retrying after 2 seconds")
		// 	time.Sleep(2 * time.Second)
		// 	continue
		// }

		// fmt.Println("Maximum retry reached, returning...")
		return
	}
}

type protocolHandler struct {
	options          *clientOptions
	wg               sync.WaitGroup
	rw               io.ReadWriter
	store            Store
	outbound         chan packet
	notifer          completionNotifier
	incomingMsgQ     *mqttutil.SyncQueue
	keepAliveTimeout time.Duration
	keepAliveTicker  *time.Ticker
	pingRespRecvd    chan struct{}
	errWhenClosed    chan<- error
	stopCloseOnce    sync.Once
	stop             chan struct{}
	mu               sync.Mutex
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
		case pkt := <-c.outbound:
			if err := c.sendPacket(pkt); err != nil {
				log.Error("Send packet returned with error ", err)
				c.shutdown(err)
				return
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

func (c *protocolHandler) publishHandler(publish *Publish) error {
	// notify the message to the subscribed client, for QoS 0, 1
	// for QoS 2 the notification happens when we send PubComp message
	msg := message{publish.TopicName, publish.Payload}

	switch publish.QoSLevel {
	case 0:
		c.incomingMsgQ.Push(&msg)
	case 1:
		pubAckPacket := &PubAck{}
		pubAckPacket.ReasonCode = PubAckReasonCodeSuccess
		pubAckPacket.packetID = publish.packetID
		c.outbound <- pubAckPacket

		c.incomingMsgQ.Push(&msg)
	case 2:
		// send a pub rec message
		pubRecPacket := &PubRec{}
		pubRecPacket.ReasonCode = PubRecReasonCode(PubRelReasonCodeSuccess)
		pubRecPacket.packetID = publish.packetID

		// store the publish packet in inbound queue, once we receive the
		// pubrel we wiill push the packet into incoming message queue for
		// consumption
		storeInBound(c.store, publish.packetID, publish)

		// store the pub rec in outbound
		storeOutbound(c.store, publish.packetID, pubRecPacket)

		// sent the packet
		c.outbound <- pubRecPacket
	}
	return nil
}

func (c *protocolHandler) pubAckHandler(pkt *PubAck) error {
	c.notifer.complete(pkt.packetID, nil, nil)
	return nil
}

func (c *protocolHandler) pubRecHandler(pubrec *PubRec) error {
	pubRelPacket := &PubRel{}
	pubRelPacket.ReasonCode = PubRelReasonCodeSuccess
	pubRelPacket.packetID = pubrec.packetID

	// store the pub rec in outbound, will overwrite the PUBLISH packet
	storeOutbound(c.store, pubRelPacket.packetID, pubRelPacket)

	c.outbound <- pubRelPacket

	return nil
}

func (c *protocolHandler) pubRelHandler(pubrel *PubRel) error {
	pubCompPacket := &PubComp{}
	pubCompPacket.ReasonCode = PubCompReasonCodeSuccess
	pubCompPacket.packetID = pubrel.packetID

	// get the publish packet and push the packet into incommng message queue
	// for consumption
	pkt := fetchInbound(c.store, pubrel.packetID)
	if pkt != nil {
		if publishPkt, ok := pkt.(*Publish); ok {
			msg := message{publishPkt.TopicName, publishPkt.Payload}
			c.incomingMsgQ.Push(&msg)
			// delete the publish packet from the store
			deleteInbound(c.store, publishPkt.packetID)
		} else {
			log.Warnf("invalid packet in storage for key %v", pubrel.packetID)
		}
	} else {
		log.Warnf("Publish packet is not found in the storage")
	}

	c.outbound <- pubCompPacket

	return nil
}

func (c *protocolHandler) pubCompHandler(pubcomp *PubComp) error {
	c.notifer.complete(pubcomp.packetID, nil, nil)
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
	writer := mqttutil.NewBufioWriterSize(c.rw, 2*1024)
	defer mqttutil.PutBufioWriter(writer)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := writeTo(pkt, writer); err != nil {
		return err
	}
	return writer.Flush()
}
