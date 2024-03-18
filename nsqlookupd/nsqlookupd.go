package nsqlookupd

import (
	"fmt"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options
	tcpListener  net.Listener
	httpListener net.Listener
	tcpServer    *tcpServer
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
}

func New(opts *Options) (*NSQLookupd, error) {
	var err error

	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	//实例化一个nsqlookupd
	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}

	l.logf(LOG_INFO, version.String("nsqlookupd"))
	//配置tcp服务，设置端口监听
	l.tcpServer = &tcpServer{nsqlookupd: l}
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
	}

	return l, nil
}

// Main starts an instance of nsqlookupd and returns an
// error if there was a problem starting up.
func (l *NSQLookupd) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				l.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}
	//开启两个协程分别负责TCP和HTTP服务
	l.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(l.tcpListener, l.tcpServer, l.logf))
	})
	httpServer := newHTTPServer(l)
	l.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(l.httpListener, httpServer, "HTTP", l.logf))
	})

	//开启一个协程来运行节点管理算法
	//每隔10秒输出所有节点信息
	l.waitGroup.Wrap(func() {
		for {
			l.ShowNodes()
			time.Sleep(10 * time.Second)
		}
	})

	err := <-exitCh
	return err
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.tcpServer != nil {
		l.tcpServer.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}

func (l *NSQLookupd) ShowNodes() {
	// dont filter out tombstoned nodes
	//不过滤逻辑删除的节点
	//查找了所有无topic无channel且在存活时间内的producer
	producers := l.DB.FindProducers("client", "", "").FilterByActive(
		l.opts.InactiveProducerTimeout, 0)
	nodes := make([]*node, len(producers))
	//话题-其生产者们map，一个话题对应了一个producers切片
	topicProducersMap := make(map[string]Producers)
	//遍历上述生产者   {p：当前生产者，topics：当前生产者的所有话题}
	for i, p := range producers {
		//根据生产者的id找到所有的Registrations，然后获得该生产者的所有话题名称
		topics := l.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			//遍历所有topic，如果topic不在topicProducersMap里
			if _, exists := topicProducersMap[t]; !exists {
				//则将该topic加入，并查找所有topic名称为该话题的producers
				topicProducersMap[t] = l.DB.FindProducers("topic", t, "")
			}
			//取得该topic下的producers
			topicProducers := topicProducersMap[t]
			//遍历这些producer，找到和当前producer相等的那个producer
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					//判断tp是否过期，状态保存到tombstones中
					tombstones[j] = tp.IsTombstoned(l.opts.TombstoneLifetime)
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
		}
	}

	l.logf(LOG_INFO, "当前所有节点：")
	for i, n := range nodes {
		l.logf(LOG_INFO, "%d 号节点:, %s", i, n.RemoteAddress)

	}
}
