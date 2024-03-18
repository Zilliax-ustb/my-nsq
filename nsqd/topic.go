package nsqd

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)

// test auto sys
type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64
	messageBytes uint64

	sync.RWMutex

	name              string                //话题名称
	channelMap        map[string]*Channel   //订阅该话题的所有频道
	backend           BackendQueue          //磁盘队列
	memoryMsgChan     chan *Message         //内存chan
	startChan         chan int              //接收开始信号的channel
	exitChan          chan int              //退出信号
	channelUpdateChan chan int              //频道更新时用来通知并更新消息循环中的chan数组
	waitGroup         util.WaitGroupWrapper //自定义子协程工具
	exitFlag          int32                 //话题退出标识符
	idFactory         *guidFactory          //生成guid的工厂方法

	ephemeral      bool         //临时话题标志
	deleteCallback func(*Topic) //topic删除时的回调函数
	deleter        sync.Once    //确保删除回调函数只执行1次

	paused    int32    //判断topic是否暂停
	pauseChan chan int //暂停通道

	nsqd *NSQD
}

// Topic constructor
// 生成新的话题 参数：话题名称，nsqd指针，一个删除回调函数    返回值：话题指针
func NewTopic(topicName string, nsqd *NSQD, deleteCallback func(*Topic)) *Topic {
	//初始化结构体内变量
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, nsqd.getOpts().MemQueueSize),
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		nsqd:              nsqd,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback,
		idFactory:         NewGUIDFactory(nsqd.getOpts().ID),
	}
	//名字以"#ephemeral"结尾的topic或channel不会落盘
	// 判断这个 topic 是不是暂时的，暂时的 topic 消息仅仅存储在内存中
	// DummyBackendQueue 和 diskqueue 均实现了 backend 接口
	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueue() // 实现了 backend 但是并没有逻辑，所有操作仅仅返回 nil
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// 使用 diskqueue 初始化 backend 队列
		t.backend = diskqueue.New(
			topicName,                      //磁盘队列名称
			nsqd.getOpts().DataPath,        //磁盘数据地址
			nsqd.getOpts().MaxBytesPerFile, //单文件最大字节数
			int32(minValidMsgLength),       //设置单条消息最短长度
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength, //设置单条消息最大长度
			nsqd.getOpts().SyncEvery,                           //每写syncEvery次数据后，系统会调用一次fsync来确保数据已经写入磁盘
			nsqd.getOpts().SyncTimeout,                         //即使没有达到syncEvery指定的写操作数量，系统也应该在syncTimeout时间到达后执行fsync
			dqLogf,                                             //自定义日志记录函数
		)
	}
	//startChan 就发送给了它,messagePump 函数负责分发整个 topic 接收到的消息给该 topic 下的 channels.
	t.waitGroup.Wrap(t.messagePump) //确保 t.messagePump(函数) 在某个协程中执行，并且可以通过 WaitGroup 来等待其完成

	t.nsqd.Notify(t, !t.ephemeral)

	return t
}

// 设置开始频道为1
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.nsqd, deleteCallback)
		t.channelMap[channelName] = channel
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.RLock()
	channel, ok := t.channelMap[channelName]
	t.RUnlock()
	if !ok {
		return errors.New("channel does not exist")
	}

	t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the channel from map below (with no lock)
	// so that any incoming subs will error and not create a new channel
	// to enforce ordering
	channel.Delete()

	t.Lock()
	delete(t.channelMap, channelName)
	numChannels := len(t.channelMap)
	t.Unlock()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	if numChannels == 0 && t.ephemeral {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		messageTotalBytes += len(m.Body)
	}

	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

// 进行消息的投递,将从tcp或http来的消息投递到topic中
func (t *Topic) put(m *Message) error {
	// If mem-queue-size == 0, avoid memory chan, for more consistent ordering,
	// but try to use memory chan for deferred messages (they lose deferred timer
	// in backend queue) or if topic is ephemeral (there is no backend queue).
	if cap(t.memoryMsgChan) > 0 || t.ephemeral || m.deferred != 0 {
		select {
		//先写入到内存中，如果内存已满，则写入到磁盘里
		case t.memoryMsgChan <- m:
			return nil
		default:
			break // write to backend
		}
	}
	err := writeMessageToBackend(m, t.backend)
	t.nsqd.SetHealth(err)
	if err != nil {
		t.nsqd.logf(LOG_ERROR,
			"TOPIC(%s) ERROR: failed to write message to backend - %s",
			t.name, err)
		return err
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte

	// do not pass messages before Start(), but avoid blocking Pause() or GetChannel()
	for { //死循环直到start通道中传出信号
		select {
		case <-t.channelUpdateChan:
			continue
		case <-t.pauseChan:
			continue
		case <-t.exitChan:
			goto exit
		case <-t.startChan:
		}
		break
	}
	t.RLock()
	//将话题中所有的频道添加到chans中
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	if len(chans) > 0 && !t.IsPaused() {
		//取出话题中的所有消息
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// main message loop
	for {
		select {
		//1.当直接可以取得消息
		case msg = <-memoryMsgChan:
		//2.当从数据通道接收数据时，需要将数据解码为信息
		case buf = <-backendChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				t.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		//3.更新频道信号
		case <-t.channelUpdateChan:
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			//如果无频道或者话题被暂停，清空数据和消息；否则便更新数据和消息
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		//		判断话题是否暂停或恢复，并设置内存队列和磁盘队列变量
		case <-t.pauseChan:
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			goto exit
		}
		//遍历所有频道
		for i, channel := range chans {
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				//如果不是第1次发送，则生成每条信息的新副本
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			//检查延迟属性
			if chanMsg.deferred != 0 {
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.name)
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}
	//根据操作写入日志
	if deleted {
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.nsqd.Notify(t, !t.ephemeral)
	} else {

		t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	// synchronize the close of messagePump()
	t.waitGroup.Wait()

	//如果是删除话题，对于删除操作，需要清空channelMap并删除所有channel
	//然后删除内存和磁盘中所有未投递的消息。最后关闭backend管理的的磁盘文件
	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	//对于关闭操作，不清空channelMap，只是关闭所有的channel，
	//使用flush函数将所有memoryMsgChan中未投递的消息用writeMessageToBackend保存到磁盘中。
	//最后关闭backend管理的的磁盘文件。
	t.RLock()
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}
	t.RUnlock()

	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

func (t *Topic) Empty() error {
	for {
		select {
		//释放队列消息
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

func (t *Topic) flush() error {
	if len(t.memoryMsgChan) > 0 {
		t.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		//取出消息并保存
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(msg, t.backend)
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			t.nsqd.logf(LOG_ERROR, "TOPIC(%s): failed to create guid - %s", t.name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}
