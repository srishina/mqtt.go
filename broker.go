package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/srishina/mqtt.go/internal/mqttutil"
)

// brokerOptions contains configurable settings for the MQTT broker
type brokerOptions struct {
	retainAvailable   bool
	maxPacketSize     uint32
	topicAliasMax     uint16
	receiveMaxValue   uint16
	certFile, keyFile string
}

var defaultBrokerOptions = brokerOptions{}

// BrokerOption configures the server
type BrokerOption func(*brokerOptions) error

// WithRetainAvailable set whether the broker should support
// retaining messages
func WithRetainAvailable(retainAvailable bool) BrokerOption {
	return func(o *brokerOptions) error {
		o.retainAvailable = retainAvailable
		return nil
	}
}

// WithMaxPacketSize maximum packet size the broker is willing to
// accept
func WithMaxPacketSize(maxPacketSize uint32) BrokerOption {
	return func(o *brokerOptions) error {
		o.maxPacketSize = maxPacketSize
		return nil
	}
}

// WithMaxTopicAlias maximum topic alias value
func WithMaxTopicAlias(aliasMax uint16) BrokerOption {
	return func(o *brokerOptions) error {
		o.topicAliasMax = aliasMax
		return nil
	}
}

// WithReceiveMaximum receive maximum value
func WithReceiveMaximum(recvMax uint16) BrokerOption {
	return func(o *brokerOptions) error {
		o.receiveMaxValue = recvMax
		return nil
	}
}

// WithCertFile configures the TLS certificate with the path
// to the certificate file
func WithCertFile(certFile string) BrokerOption {
	return func(o *brokerOptions) error {
		o.certFile = certFile
		return nil
	}
}

// WithKeyFile configures the TLS key with the path to the key file
func WithKeyFile(keyFile string) BrokerOption {
	return func(o *brokerOptions) error {
		o.keyFile = keyFile
		return nil
	}
}

// Broker represents a broker object
type Broker struct {
	mu            sync.Mutex
	url           url.URL
	settings      brokerOptions
	httpServer    *http.Server
	tcpListener   net.Listener
	activeConn    map[*conn]struct{}
	activeClients map[string]*conn
	topicMatcher  *mqttutil.TopicMatcher
	wg            sync.WaitGroup
}

// NewBroker creates a new broker instance
func NewBroker(url *url.URL, opt ...BrokerOption) *Broker {
	opts := defaultBrokerOptions

	for _, o := range opt {
		o(&opts)
	}
	return &Broker{url: *url,
		settings:     opts,
		topicMatcher: mqttutil.NewTopicMatcher(),
	}
}

// Start start the broker with the configured options
func (b *Broker) Start() error {

	switch b.url.Scheme {
	// case "ws":
	// 	fallthrough
	// case "wss":
	// 	return s.serveWebsocket(&s.url, s.settings.CertFile, s.settings.KeyFile)
	case "tcp":
		fallthrough
	case "tcps":
		return b.serveTCP(&b.url)
	default:
		log.Fatal("unsupported scheme")
	}

	fmt.Printf("serve ended...\n")
	return nil
}

// Stop stop the broker
func (b *Broker) Stop() error {
	if b.tcpListener != nil {
		b.tcpListener.Close()
	}

	if b.httpServer != nil {
		b.httpServer.Shutdown(context.Background())
	}

	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		b.wg.Wait()
	}()

	select {
	case <-waitCh:
	case <-time.After(60 * time.Second):
		// return fmt.Errorf("Error: The server refuses to shutdown. No. of active connections: %d", s.numberOfActiveConns())
	}
	// fmt.Printf("Number of active connections %d\n", s.numberOfActiveConns())
	return nil
}

func (b *Broker) traceClient(sessionID string, c *conn, add bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.activeClients == nil {
		b.activeClients = make(map[string]*conn)
	}
	if add {
		b.activeClients[sessionID] = c
	} else {
		delete(b.activeClients, sessionID)
	}
}

func (b *Broker) hasClient(sessionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.activeClients[sessionID]; ok {
		return true
	}

	return false
}

func (b *Broker) getClient(sessionID string) *conn {
	b.mu.Lock()
	defer b.mu.Unlock()
	if client, ok := b.activeClients[sessionID]; ok {
		return client
	}

	return nil
}

func newTCPListener(url *url.URL, certFile, keyFile string) (net.Listener, error) {
	var err error
	var netln net.Listener
	if certFile != "" && keyFile != "" {
		config := tls.Config{}
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		netln, err = tls.Listen("tcp", url.Host, &config)
	} else {
		netln, err = net.Listen("tcp", url.Host)
	}

	return netln, err
}

func (b *Broker) serveTCP(url *url.URL) error {
	netln, err := newTCPListener(url, "", "")
	if err != nil {
		return err
	}

	b.tcpListener = netln
	defer b.tcpListener.Close()

	fmt.Println("Waiting to accept new connection")

	var tempDelay time.Duration
	for {
		rw, err := netln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Infof("Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			fmt.Printf("Accept failed returning with err %v\n", err)
			return err
		}
		log.Infof("New connection is accepted, serving...")
		newConn := newConnection(b, rw)
		b.wg.Add(1)
		go serveConnection(&b.wg, newConn)
	}
}

func (b *Broker) trackConn(c *conn, add bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.activeConn == nil {
		b.activeConn = make(map[*conn]struct{})
	}
	if add {
		b.activeConn[c] = struct{}{}
	} else {
		delete(b.activeConn, c)
	}
}

func (b *Broker) numberOfActiveConns() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.activeConn)
}

type connState int

const (
	stateNew connState = iota
	stateActive
	stateIdle
	stateClosed
)

// publisher publishes MQTT message
type publisher interface {
	publish(id uint32, subscription *Subscription, pkt *Publish) error
}

type serverSubscription struct {
	id           uint32
	subscription *Subscription
	publisher    publisher
	// todo.. check if it makes sense, we will be able to
	// publish async, currently when we receive a packet
	// we publish at that point
}

func (s *serverSubscription) notify(pkt *Publish) error {
	return s.publisher.publish(s.id, s.subscription, pkt)
}

func (s *serverSubscription) Finalize() {
}

type session struct {
	clientID       string
	userName       string
	password       []byte
	expiryInterval uint32
	recvMaximum    uint16
	maxPacketSize  uint32
	authMethod     string
	authData       []byte
	mu             sync.Mutex
	subscriptions  map[string]*serverSubscription
}

type conn struct {
	broker        *Broker
	rwc           net.Conn
	session       *session
	state         uint32 // accessed atomically.
	stopCloseOnce sync.Once
	stop          chan struct{}
	outbound      chan controlPacket
	pidgenerator  *mqttutil.PIDGenerator
}

func newConnection(brkr *Broker, rwc net.Conn) *conn {
	return &conn{
		broker:   brkr,
		rwc:      rwc,
		outbound: make(chan controlPacket, 4),
		stop:     make(chan struct{}),
		session: &session{
			subscriptions: make(map[string]*serverSubscription),
		},
		pidgenerator: mqttutil.NewPIDGenerator(),
	}
}

func serveConnection(wg *sync.WaitGroup, c *conn) {
	defer wg.Done()
	c.setState(stateNew)
	c.serve()
}

func (c *conn) serve() error {
	defer func() {
		c.close()
		close(c.outbound) // close the outbound channel
		c.setState(stateClosed)
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go c.receiver(&wg)
	go c.sender(&wg)

	// wait for completion...
	wg.Wait()

	return nil
}

// Close the network connection
func (c *conn) close() {
	c.stopCloseOnce.Do(func() {
		c.rwc.Close() // close the connection
		close(c.stop) // close the sender
	})
}

func (c *conn) sendPacket(pkt controlPacket) error {
	writer := mqttutil.NewBufioWriterSize(c.rwc, 2*1024)
	defer mqttutil.PutBufioWriter(writer)
	if err := writeTo(pkt, writer); err != nil {
		return err
	}
	return writer.Flush()
}

func (c *conn) sender(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case pkt := <-c.outbound:
			if err := c.sendPacket(pkt); err != nil {
				log.Infof("Send packet returned with error %v", err)
				return
			}
		case <-c.stop:
			return
		}
	}
}

func (c *conn) receiver(wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.close()
	for {
		pkt, err := readFrom(c.rwc)
		if err != nil {
			return
		}
		if err := c.process(pkt); err != nil {
			return
		}
	}
}

func (c *conn) process(pkt controlPacket) error {
	switch pkt.(type) {
	case *Connect:
		return c.connectHandler(pkt.(*Connect))
	case *Disconnect:
		return c.disconnectHandler(pkt.(*Disconnect))
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
	case *Subscribe:
		return c.subscribeHandler(pkt.(*Subscribe))
	case *Unsubscribe:
		return c.unSubscribeHandler(pkt.(*Unsubscribe))
	case *pingReq:
		return c.pingReqHandler(pkt.(*pingReq))
	default:
		// unrecognized message
	}
	return ErrProtocol
}

func (c *conn) setState(state connState) {
	switch state {
	case stateNew:
		c.broker.trackConn(c, true)
	case stateClosed:
		c.broker.trackConn(c, false)
	}

	atomic.StoreUint32(&c.state, uint32(state))
}

func (c *conn) getState() connState {
	state := atomic.LoadUint32(&c.state)
	return connState(state)
}

func (c *conn) connectHandler(connect *Connect) error {
	connAckProperties := &ConnAckProperties{}

	if c.getState() != stateNew {
		log.Infof("Bad client %s sending multiple CONNECT messages.", c.session.clientID)
		return ErrProtocol
	}

	if connect.protocolVersion != PROTOCOLVERSIONv5 {
		// 	// send conn ack
		log.Infof("Unsupported protocol version %d in CONNECT from %s.",
			connect.protocolVersion, c.rwc.LocalAddr().String())

		c.sendConnAck(&c.broker.settings, false, ConnAckReasonCodeUnsupportedProtocolVer, nil)
		return ErrProtocol
	}

	if connect.WillFlag && !c.broker.settings.retainAvailable {
		// 	// "the Will Flag is set to 1 and Will Retain is set to 1,
		// 	// the Server MUST publish the Will Message as a retained message" [MQTT-3.1.2-15]
		// 	// The server does not support retain
		c.sendConnAck(&c.broker.settings, false, ConnAckReasonCodeRetainNotSupported, nil)
		return ErrProtocol
	}

	// check client id
	if len(connect.ClientID) == 0 {
		// 	// generate a unique client id and assign, MQTT-3.1.3-6
		c.session.clientID = "auto-" + uuid.New().String()
		connAckProperties.AssignedClientIdentifier = c.session.clientID
	} else {
		c.session.clientID = connect.ClientID
	}

	sessionPresent := false
	if connect.CleanStart {
		// check if a client already exists, then disconnect
		if client := c.broker.getClient(c.session.clientID); client != nil {
			// Already exist, disconnect with "Session Taken Over" reason
			client.disconnect(DisconnectReasonCodeSessionTakenOver)
		}
	} else {
		// check if a session already exists
		sessionPresent = c.broker.hasClient(c.session.clientID)
	}

	// add the client session
	c.broker.traceClient(c.session.clientID, c, true)

	// username and password
	if len(connect.UserName) > 0 {
		c.session.userName = connect.UserName
	}

	if len(connect.Password) > 0 {
		c.session.password = connect.Password
	}

	if connect.Properties != nil {
		if connect.Properties.SessionExpiryInterval != nil {
			c.session.expiryInterval = *connect.Properties.SessionExpiryInterval
		}

		if connect.Properties.ReceiveMaximum != nil {
			if *connect.Properties.ReceiveMaximum == 0 {
				c.sendConnAck(&c.broker.settings, false, ConnAckReasonCodeProtocolError, connAckProperties)
				return ErrProtocol
			}
			c.session.recvMaximum = *connect.Properties.ReceiveMaximum
		}

		if connect.Properties.MaximumPacketSize != nil {
			if *connect.Properties.MaximumPacketSize == 0 {
				c.sendConnAck(&c.broker.settings, false, ConnAckReasonCodeProtocolError, connAckProperties)
				return ErrProtocol
			}
			c.session.maxPacketSize = *connect.Properties.MaximumPacketSize
		}

		c.session.authMethod = connect.Properties.AuthenticationMethod
		c.session.authData = connect.Properties.AuthenticationData
	}

	c.setState(stateActive)

	return c.sendConnAck(&c.broker.settings, sessionPresent, ConnAckReasonCodeSuccess, connAckProperties)
}

func (c *conn) sendConnAck(options *brokerOptions, sessionPresent bool, reasonCode ConnAckReasonCode, properties *ConnAckProperties) error {
	connAckPkt := &ConnAck{
		SessionPresent: false,
		ReasonCode:     reasonCode,
		Properties:     properties,
	}
	var err error
	if reasonCode >= ConnAckReasonCodeUnspecifiedError {
		err = ErrProtocol
	} else {
		// A value of 1 means retained messages are supported. If the retain available
		// property is not present, then retained messages are supported
		if !options.retainAvailable {
			connAckPkt.Properties.RetainAvailable = &options.retainAvailable
		}

		if options.maxPacketSize > 0 {
			connAckPkt.Properties.MaximumPacketSize = &options.maxPacketSize
		}

		if options.topicAliasMax > 0 {
			connAckPkt.Properties.TopicAliasMaximum = &options.topicAliasMax
		}

		if options.receiveMaxValue > 0 {
			connAckPkt.Properties.ReceiveMaximum = &options.receiveMaxValue
		}
	}

	// send the connack packet
	c.outbound <- connAckPkt
	return err
}

func (c *conn) disconnectHandler(disconnect controlPacket) error {
	// close the network connection
	defer c.close()

	log.Info("Received DISCONNECT from client: ", c.session.clientID)

	return nil
}

func (c *conn) subscribeHandler(subscribe *Subscribe) error {
	if c.getState() != stateActive {
		return ErrProtocol
	}

	log.Info("Received SUBSCRIBE from client: ", c.session.clientID)
	// // The Payload MUST contain at least one Topic Filter and Subscription Options pair [MQTT-3.8.3-2]
	if len(subscribe.Subscriptions) == 0 {
		return ErrProtocol
	}

	subid := uint32(0)
	if subscribe.Properties != nil {
		// read the subscription identifier property
		if subscribe.Properties.SubscriptionIdentifier != nil {
			if *subscribe.Properties.SubscriptionIdentifier == 0 {
				return ErrProtocol
			}
			subid = *subscribe.Properties.SubscriptionIdentifier
		}
	}

	subAckPayload := make([]SubAckReasonCode, len(subscribe.Subscriptions))
	for i, subscription := range subscribe.Subscriptions {
		svrSubscription := &serverSubscription{id: subid, subscription: subscription, publisher: c}

		if err := c.broker.topicMatcher.Subscribe(subscription.TopicFilter, svrSubscription); err != nil {
			log.Errorf("Topic subscription failed %v", err)
			return err
		}

		c.addSubscription(subscription.TopicFilter, svrSubscription)

		// add to subscription cache
		subAckPayload[i] = SubAckReasonCode(subscription.QoSLevel)
	}

	return c.sendSubAck(subscribe.packetID, subAckPayload)
}

func (c *conn) sendSubAck(packetid uint16, payload []SubAckReasonCode) error {
	subAckPkt := &SubAck{packetID: packetid, ReasonCodes: payload}
	// send the suback packet
	c.outbound <- subAckPkt
	return nil
}

func (c *conn) addSubscription(topic string, subscription *serverSubscription) error {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	c.session.subscriptions[topic] = subscription
	return nil
}

func (c *conn) removeSubscription(topic string) error {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	delete(c.session.subscriptions, topic)
	return nil
}

func (c *conn) getSubscription(topic string) *serverSubscription {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	return c.session.subscriptions[topic]
}

func (c *conn) unSubscribeHandler(unsubscribe *Unsubscribe) error {
	if c.getState() != stateActive {
		return ErrProtocol
	}

	log.Info("Received UNSUBSCRIBE from client: ", c.session.clientID)
	// The Payload MUST contain at least one Topic Filter and Subscription Options pair [MQTT-3.8.3-2]
	if len(unsubscribe.TopicFilters) == 0 {
		return ErrProtocol
	}

	unsubAckPayload := make([]UnsubAckReasonCode, len(unsubscribe.TopicFilters))
	for i, topicFilter := range unsubscribe.TopicFilters {
		subscription := c.getSubscription(topicFilter)
		if subscription == nil {
			unsubAckPayload[i] = UnsubAckNoSubscriptionExisted
			continue
		}
		if err := c.broker.topicMatcher.Unsubscribe(topicFilter, subscription); err != nil {
			log.Info("Invalid subscription string from ", c.rwc.LocalAddr().String(), " disconnecting...")
			return err
		}

		// remove subscription from cache
		c.removeSubscription(topicFilter)

		unsubAckPayload[i] = UnsubAckReasonCodeSuccess
	}

	return c.sendUnsubAck(unsubscribe.packetID, unsubAckPayload)
}

func (c *conn) sendUnsubAck(packetid uint16, payload []UnsubAckReasonCode) error {
	unsubAckPkt := &UnsubAck{packetID: packetid, ReasonCodes: payload}
	// send the unsuback packet
	c.outbound <- unsubAckPkt
	return nil
}

func (c *conn) publishHandler(publish *Publish) error {
	if c.getState() != stateActive {
		return ErrProtocol
	}

	log.Infof("Received PUBLISH from client: %s QoS Level: %d ", c.session.clientID, publish.QoSLevel)
	if publish.Retain && !c.broker.settings.retainAvailable {
		// todo retain is not supported
		return ErrProtocol
	}

	var subscribers []mqttutil.Subscriber
	c.broker.topicMatcher.Match(publish.TopicName, &subscribers)

	fmt.Println("number of subscribers ", len(subscribers))
	for _, subscriber := range subscribers {
		if subscription, ok := subscriber.(*serverSubscription); ok {
			subscription.notify(publish)
		}
	}

	if publish.QoSLevel == 1 {
		// send puback packet
		pubAckPacket := &PubAck{packetID: publish.packetID}
		if len(subscribers) == 0 {
			pubAckPacket.ReasonCode = PubAckReasonCodeNoMatchingSubscribers
		} else {
			pubAckPacket.ReasonCode = PubAckReasonCodeSuccess
		}
		c.outbound <- pubAckPacket
	} else if publish.QoSLevel == 2 {
		// send pubrec packet
		pubRecPacket := &PubRec{packetID: publish.packetID}
		if len(subscribers) == 0 {
			pubRecPacket.ReasonCode = PubRecReasonCodeNoMatchingSubscribers
		} else {
			pubRecPacket.ReasonCode = PubRecReasonCodeSuccess
		}
		c.outbound <- pubRecPacket
	}

	return nil
}

func (c *conn) pubAckHandler(puback *PubAck) error {
	log.Info("Received PUBACK for id ", puback.packetID)
	// release the packet ID
	c.pidgenerator.FreeID(puback.packetID)
	return nil
}

func (c *conn) pubRecHandler(pubrec *PubRec) error {
	log.Info("Received PUBREC for id ", pubrec.packetID)
	pubRelPacket := &PubRel{packetID: pubrec.packetID, ReasonCode: PubRelReasonCodeSuccess}
	c.outbound <- pubRelPacket
	return nil
}

func (c *conn) pubRelHandler(pubrel *PubRel) error {
	log.Info("Received PUBREL for id ", pubrel.packetID)
	pubCompPacket := &PubComp{packetID: pubrel.packetID, ReasonCode: PubCompReasonCodeSuccess}
	c.outbound <- pubCompPacket
	return nil
}

func (c *conn) pubCompHandler(pubcomp *PubComp) error {
	log.Info("Received PUBCOMP for id ", pubcomp.packetID)
	// release the packet ID
	c.pidgenerator.FreeID(pubcomp.packetID)
	return nil
}

func (c *conn) pingReqHandler(pingreq *pingReq) error {
	if c.getState() != stateActive {
		return ErrProtocol
	}

	// we are alive, send a ping response
	return c.sendPingResponse()
}

func (c *conn) sendPingResponse() error {
	// send the pingresp packet
	c.outbound <- &pingResp{}
	return nil
}

func (c *conn) publish(id uint32, s *Subscription, pkt *Publish) error {
	qos := pkt.QoSLevel
	if qos > s.QoSLevel {
		qos = s.QoSLevel
	}

	publishPkt := &Publish{
		TopicName: pkt.TopicName,
		QoSLevel:  qos,
		Payload:   pkt.Payload,
	}

	if s.QoSLevel > 0 {
		publishPkt.packetID = c.pidgenerator.NextID()
	}

	if id > 0 {
		publishPkt.Properties = &PublishProperties{SubscriptionIdentifiers: []uint32{id}}
	}

	c.outbound <- publishPkt
	return nil
}

func (c *conn) disconnect(reasonCode DisconnectReasonCode) error {
	disconnectPkt := &Disconnect{ReasonCode: reasonCode}
	c.outbound <- disconnectPkt
	return nil
}
