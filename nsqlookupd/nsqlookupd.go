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
	"os/signal"
	"sync"
	"syscall"
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

	ec := make(chan os.Signal, 1)
	signal.Notify(ec, syscall.SIGINT, syscall.SIGTERM)
	//开启一个协程来运行节点管理算法
	//每隔15秒输出所有节点信息(包括游离态节点)
	l.waitGroup.Wrap(func() {
		ticker := time.NewTicker(15 * time.Second)
		for {
			select {
			case <-ec:
				l.logf(LOG_INFO, "程序结束，nsqlookupd已退出")
				return
			case <-ticker.C:
				l.ShowNodes()
			}
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

// 输出所有节点信息(包括游离态节点)
func (l *NSQLookupd) ShowNodes() {
	//查找所有节点
	producers := l.DB.FindProducers("client", "", "")
	nodes := make([]*node, len(producers))

	//遍历上述节点   {p：当前节点，topics：该节点的所有话题}
	for i, p := range producers {
		//根据节点的id找到该节点所有的话题
		topics := l.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		// 遍历当前节点的每个话题，检查其生产者是否被逻辑删除
		tombstones := make([]bool, len(topics))
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
			IpAddress:        p.peerInfo.IpAddress,
		}
	}

	l.logf(LOG_INFO, "当前所有节点:")
	for i, n := range nodes {
		l.logf(LOG_INFO, "(%d)号节点: %s, 游离状态: %d", i, n.IpAddress, n.Free)
	}
}
