package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

type LookupProtocolV1 struct {
	nsqlookupd *NSQLookupd
}

func (p *LookupProtocolV1) NewClient(conn net.Conn) protocol.Client {
	return NewClientV1(conn)
}

func (p *LookupProtocolV1) IOLoop(c protocol.Client) error {
	var err error
	var line string

	client := c.(*ClientV1)

	reader := bufio.NewReader(client)

	for {
		//读取用户命令
		line, err = reader.ReadString('\n') //断开连接错误由此产生，并直接跳出循环
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		//按空格切割用户请求，params存储的就是用户请求命令以及参数
		params := strings.Split(line, " ")

		var response []byte
		//执行请求并返回结果
		response, err = p.Exec(client, reader, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			_, sendErr := protocol.SendResponse(client, []byte(err.Error()))
			if sendErr != nil {
				p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	//需要判断err是否是断开连接，据此设置游离态
	if strings.Contains(err.Error(), "An existing connection was forcibly closed by the remote host.") {
		p.nsqlookupd.logf(LOG_INFO, "检测到节点(%s)断开连接,已将其设置为游离态", client.peerInfo.IpAddress)
		//设置节点为游离态
		atomic.StoreInt64(&client.peerInfo.free, 1)
		//如果是首次断开,初始化结构体，并更新连接时长数组
		if client.peerInfo.freeNodeInfo == nil {
			fnInfo := &FreeNodeInfo{
				ReconnectCount:       0,
				ReconnectionInterval: [10]float64{},
				rFont:                0,
				rRear:                0,
				rSize:                0,
				ConnectedInterval:    [10]float64{},
				cFont:                0,
				cRear:                0,
				cSize:                0,
			}
			client.peerInfo.freeNodeInfo = fnInfo
			client.peerInfo.freeNodeInfo.updateC(atomic.LoadInt64(&client.peerInfo.ConnectDate))
		} else {
			//如果不是首次断开，只更新连接时长队列
			client.peerInfo.freeNodeInfo.updateC(atomic.LoadInt64(&client.peerInfo.ConnectDate))
		}
		//记录下节点断开连接的时间
		atomic.StoreInt64(&client.peerInfo.lastUpdate, time.Now().UnixNano())
		return nil
	}

	p.nsqlookupd.logf(LOG_INFO, "PROTOCOL(V1): [%s] exiting ioloop", client)
	//在出现与客户端通信错误后，删除该客户端的所有注册信息
	if client.peerInfo != nil {
		registrations := p.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
		p.nsqlookupd.logf(LOG_INFO, "客户端(%s)已断开连接并退出", client)
	}
	return err
}

func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	//ping命令，发送心跳，保持连接
	case "PING":
		return p.PING(client, params)
	//当nsqd第一次连接nsqlookupd时，发送IDENTITY，验证自己身份
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	//当nsqd创建一个topic或者channel时，向nsqlookupd发送REGISTER请求
	//在nsqlookupd上更新当前nsqd的topic或者channel信息
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	//当nsqd删除一个topic或者channel时，向nsqlookupd发送UNREGISTER请求
	//在nsqlookupd上更新当前nsqd的topic或者channel信息
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	key := Registration{"topic", topic, ""}
	if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// for ephemeral channels, remove the channel as well if it has no producers
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else {
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed
		registrations := p.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id)
			if removed {
				p.nsqlookupd.logf(LOG_WARN, "client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{"topic", topic, ""}
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
		if left == 0 && strings.HasSuffix(topic, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key)
		}
	}

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.peerInfo != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	//首先设置nsq节点的id
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}

	//将json格式的body解码存入到peerInfo
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	//判断该节点是否是游离的节点，如果是则找回之前的注册信息
	//找到所有游离态节点
	allFreeNodes := p.nsqlookupd.DB.FindAllFreeNodes()
	client_IP := strings.Split(peerInfo.id, ":")[0] + ":" + strconv.Itoa(peerInfo.TCPPort)
	for _, n := range allFreeNodes {
		//如果确定是之前游离态的节点
		if client_IP == n.peerInfo.IpAddress {
			//将节点信息找回
			client.peerInfo = n.peerInfo
			//更新节点id，id每次重连都会改变
			client.peerInfo.id = peerInfo.id
			//更新节点的远程地址
			client.peerInfo.RemoteAddress = client.RemoteAddr().String()

			//取消节点的游离态
			atomic.StoreInt64(&client.peerInfo.free, 0)
			//更新节点重连次数
			client.peerInfo.freeNodeInfo.ReconnectCount = client.peerInfo.freeNodeInfo.ReconnectCount + 1
			//更新节点断连时长队列
			client.peerInfo.freeNodeInfo.updateR(atomic.LoadInt64(&client.peerInfo.lastUpdate))
			//更新节点的上次响应时间为现在
			atomic.StoreInt64(&client.peerInfo.lastUpdate, time.Now().UnixNano())
			atomic.StoreInt64(&client.peerInfo.ConnectDate, time.Now().UnixNano())

			p.nsqlookupd.logf(LOG_INFO, "游离态节点:%s 已重新连接", client.peerInfo.IpAddress)
			p.nsqlookupd.logf(LOG_INFO, "节点已有断连间隔%d次", client.peerInfo.freeNodeInfo.rSize)
			t := client.peerInfo.freeNodeInfo.rFont
			for i := 0; i < client.peerInfo.freeNodeInfo.rSize; i++ {
				p.nsqlookupd.logf(LOG_INFO, "第%d次断开间隔为%f秒", i, client.peerInfo.freeNodeInfo.ReconnectionInterval[t])
				t = (t + 1) % 10

			}
			p.nsqlookupd.logf(LOG_INFO, "节点断连时间方差为：%f", client.peerInfo.freeNodeInfo.getRIvariance())

			// build a response
			data := make(map[string]interface{})
			data["tcp_port"] = p.nsqlookupd.RealTCPAddr().Port
			data["http_port"] = p.nsqlookupd.RealHTTPAddr().Port
			data["version"] = version.Binary
			hostname, err := os.Hostname()
			if err != nil {
				log.Fatalf("ERROR: unable to get hostname %s", err)
			}
			data["broadcast_address"] = p.nsqlookupd.opts.BroadcastAddress
			data["hostname"] = hostname

			//将nsqlookupd的信息回传给nsqd
			response, err := json.Marshal(data)
			if err != nil {
				p.nsqlookupd.logf(LOG_ERROR, "marshaling %v", data)
				return []byte("OK"), nil
			}
			return response, nil
		}
	}

	//设置节点的ip地址信息
	peerInfo.RemoteAddress = client.RemoteAddr().String()

	// require all fields
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	//更新节点最新响应时间戳
	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())
	atomic.StoreInt64(&peerInfo.ConnectDate, time.Now().UnixNano())

	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version)

	//将节点的游离态设置为0，表示当前节点在线
	atomic.StoreInt64(&peerInfo.free, 0)
	//设置节点的唯一标识
	peerInfo.IpAddress = client_IP

	//将节点信息赋值
	client.peerInfo = &peerInfo
	if p.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = p.nsqlookupd.RealTCPAddr().Port
	data["http_port"] = p.nsqlookupd.RealHTTPAddr().Port
	data["version"] = version.Binary
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.nsqlookupd.opts.BroadcastAddress
	data["hostname"] = hostname

	//将nsqlookupd的信息回传给nsqd
	response, err := json.Marshal(data)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		// we could get a PING before other commands on the same client connection
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
			now.Sub(cur))
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return []byte("OK"), nil
}
