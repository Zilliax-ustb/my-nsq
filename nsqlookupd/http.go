package nsqlookupd

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

type httpServer struct {
	nsqlookupd *NSQLookupd
	router     http.Handler
}

func newHTTPServer(l *NSQLookupd) *httpServer {
	log := http_api.Log(l.logf)

	//实例化一个http路由
	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	//设置panic时的处理函数
	router.PanicHandler = http_api.LogPanicHandler(l.logf)
	//设置not found处理函数
	router.NotFound = http_api.LogNotFoundHandler(l.logf)
	//当请求方法不支持时的处理函数
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(l.logf)
	s := &httpServer{
		nsqlookupd: l,
		router:     router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))

	// v1 negotiate
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.V1))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, log, http_api.V1))
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.V1))
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.V1))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.V1))

	// only v1
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	// debug
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}

func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topics := s.nsqlookupd.DB.FindRegistrations("topic", "*", "").Keys()
	return map[string]interface{}{
		"topics": topics,
	}, nil
}

func (s *httpServer) doChannels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	channels := s.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	return map[string]interface{}{
		"channels": channels,
	}, nil
}

// 查询特定主题（topic）的注册信息、通道（channel）列表以及生产者（producer）列表
func (s *httpServer) doLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registration := s.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	//找到该主题下的所有频道和生产者
	channels := s.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.nsqlookupd.opts.InactiveProducerTimeout,
		s.nsqlookupd.opts.TombstoneLifetime)
	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}

func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	if !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	s.nsqlookupd.logf(LOG_INFO, "DB: adding topic(%s)", topicName)
	key := Registration{"topic", topicName, ""}
	s.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registrations := s.nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		s.nsqlookupd.logf(LOG_INFO, "DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		s.nsqlookupd.DB.RemoveRegistration(registration)
	}

	registrations = s.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		s.nsqlookupd.logf(LOG_INFO, "DB: removing topic(%s)", topicName)
		s.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

// 将特定主题的生产者标记为“墓碑”
func (s *httpServer) doTombstoneTopicProducer(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	node, err := reqParams.Get("node")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}

	s.nsqlookupd.logf(LOG_INFO, "DB: setting tombstone for producer@%s of topic(%s)", node, topicName)
	//找到特定主题的生产者们
	producers := s.nsqlookupd.DB.FindProducers("topic", topicName, "")
	for _, p := range producers {
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		//如果这些生产者与返回的节点相同，则设置为“墓碑”   （返回的node中已经忽略了已经是“墓碑”的节点）
		if thisNode == node {
			p.Tombstone()
		}
	}

	return nil, nil
}

func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.nsqlookupd.logf(LOG_INFO, "DB: adding channel(%s) in topic(%s)", channelName, topicName)
	key := Registration{"channel", topicName, channelName}
	s.nsqlookupd.DB.AddRegistration(key)

	s.nsqlookupd.logf(LOG_INFO, "DB: adding topic(%s)", topicName)
	key = Registration{"topic", topicName, ""}
	s.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	registrations := s.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	s.nsqlookupd.logf(LOG_INFO, "DB: removing channel(%s) from topic(%s)", channelName, topicName)
	for _, registration := range registrations {
		s.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

type node struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
	Free             int64
	IpAddress        string
}

func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// dont filter out tombstoned nodes
	//不过滤逻辑删除的节点
	//查找所有节点
	producers := s.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.nsqlookupd.opts.InactiveProducerTimeout, 0)
	nodes := make([]*node, len(producers))
	//话题-其生产者们map，一个话题对应了一个producers切片
	topicProducersMap := make(map[string]Producers)
	//遍历上述节点   {p：当前节点，topics：该节点的所有话题}
	for i, p := range producers {
		//根据节点的id找到该节点所有的话题
		topics := s.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		// 遍历当前节点的每个话题，检查其生产者是否被逻辑删除
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			//遍历所有topic，如果topic不在topicProducersMap里
			if _, exists := topicProducersMap[t]; !exists {
				//则将该topic加入，并查找所有topic名称为该话题的producers
				topicProducersMap[t] = s.nsqlookupd.DB.FindProducers("topic", t, "")
			}
			//取得该topic下的producers
			topicProducers := topicProducersMap[t]
			//遍历这些producer，找到和当前producer相等的那个producer
			for _, tp := range topicProducers {
				//从该话题的所有节点中找到当前节点
				if tp.peerInfo == p.peerInfo {
					//判断tp是否过期，状态保存到tombstones中
					tombstones[j] = tp.IsTombstoned(s.nsqlookupd.opts.TombstoneLifetime)
					break
				}
			}
		}
		//当检查完该producer所有的topics后
		nodes[i] = &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TCPPort:          p.peerInfo.TCPPort,
			HTTPPort:         p.peerInfo.HTTPPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
			Free:             p.peerInfo.free,
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

func (s *httpServer) doDebug(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.nsqlookupd.DB.RLock()
	defer s.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range s.nsqlookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		for _, p := range producers {
			m := map[string]interface{}{
				"id":                p.peerInfo.id,
				"hostname":          p.peerInfo.Hostname,
				"broadcast_address": p.peerInfo.BroadcastAddress,
				"tcp_port":          p.peerInfo.TCPPort,
				"http_port":         p.peerInfo.HTTPPort,
				"version":           p.peerInfo.Version,
				"last_update":       atomic.LoadInt64(&p.peerInfo.lastUpdate),
				"tombstoned":        p.tombstoned,
				"tombstoned_at":     p.tombstonedAt.UnixNano(),
			}
			data[key] = append(data[key], m)
		}
	}

	return data, nil
}
