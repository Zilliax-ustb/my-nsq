package nsqlookupd

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]ProducerMap
}

// nsqd 注册信息
type Registration struct {
	Category string // 类别，client、topic、channel。其中client则表示nsqd在第一次连接nsqlookup的IDENTITY验证信息
	Key      string //topic 名称
	SubKey   string //channel 名称
}
type Registrations []Registration

type FreeNodeInfo struct {
	ReconnectCount       int         //重连次数
	ReconnectionInterval [10]float64 //重连时间间隔,单位：s
	rFont                int         //队列头
	rRear                int         //队列尾
	rSize                int         //队列长度
	ConnectedInterval    [10]float64 //连接时长,单位：s
	cFont                int         //队列头
	cRear                int         //队列尾
	cSize                int         //队列长度
}

// nsq节点信息  ***核心部分***
type PeerInfo struct {
	lastUpdate       int64         //nsqd 上次ping的时间
	id               string        //nsqd连接唯一ID，代表此次连接nsq节点的ip地址
	RemoteAddress    string        `json:"remote_address"`    //ip地址
	Hostname         string        `json:"hostname"`          //主机名称
	BroadcastAddress string        `json:"broadcast_address"` //广播地址
	TCPPort          int           `json:"tcp_port"`          //tcp接口
	HTTPPort         int           `json:"http_port"`         //http接口
	Version          string        `json:"version"`           //nsqd版本
	free             int64         //游离标识，用于标记该节点暂离状态
	IpAddress        string        //nsq节点的ip地址，用于确定唯一的nsq
	ConnectDate      int64         //每次连接时的时间,用于计算该次连接时长
	freeNodeInfo     *FreeNodeInfo //节点游离参数保存

}

/*
逻辑删除：让某一个topic不在集群中的某个nsqd上生产
在TombstoneLifetime时间周期内（默认45s），生产者不会在/lookup查询中列出该nsqd节点，并阻止consumer重新发现这个nsqd节点
*/

type Producer struct {
	peerInfo     *PeerInfo //nsqd节点相关信息
	tombstoned   bool      //nsqd是否被逻辑删除 标志
	tombstonedAt time.Time //被标识为tombstone的时间点
}

type Producers []*Producer
type ProducerMap map[string]*Producer

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

// 将生产者标记为逻辑删除
func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

// 判断生产者是否被逻辑删除
// 只有被标识为逻辑删除并且
func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Since(p.tombstonedAt) < lifetime
}

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap),
	}
}

// add a registration key
// 当nsqd创建topic或channel时注册到nsqlookup
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
}

// add a producer to a registration
// 添加生产者
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
	producers := r.registrationMap[k]
	_, found := producers[p.peerInfo.id]
	if !found {
		producers[p.peerInfo.id] = p
	}
	return !found
}

// remove a producer from a registration
// 删除生产者
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false
	if _, exists := producers[id]; exists {
		removed = true
	}

	// Note: this leaves keys in the DB even if they have empty lists
	delete(producers, id)
	return removed, len(producers)
}

// remove a Registration and all it's producers
// 当nsqd删除topic或channel时从nsqlookup中删除
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

// 当topic名称或者channel名称有一个为*时，该RegistrationDB需要过滤器
func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

// 查找注册信息
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	//如果r不需要过滤器
	if !r.needFilter(key, subkey) {
		//根据参数生成一个新的Registration
		k := Registration{category, key, subkey}
		//如果存在这个Registration，则返回包含k的Registrations切片
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}
		//如果不存在，则返回空的切片
		return Registrations{}
	}
	//如果r需要过滤器（即要查找的registration中topic名称或者channel名称为*{任意}）
	//生成一个空的Registrations切片
	results := Registrations{}
	//遍历每个registration，检查条件，将满足的加入到results中
	for k := range r.registrationMap {
		//相当于查找相同类别的registration
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

// 查找生产者
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	//如果要查找的有具体的topic和channel名称
	if !r.needFilter(key, subkey) {
		//生成新的registration
		k := Registration{category, key, subkey}
		//传入匹配registration的producermap，将这个map变为切片
		return ProducerMap2Slice(r.registrationMap[k])
	}
	//如果有*，生成一个results，用于辅助结果生成
	results := make(map[string]struct{})
	//生成一个producer指针切片
	var retProducers Producers
	//遍历[registration,producerMap]，取出registration查看是否匹配
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		//对于匹配的registration，遍历其producermap
		for _, producer := range producers {
			//将未加入的producer加入到retProducers中
			_, found := results[producer.peerInfo.id]
			if !found {
				results[producer.peerInfo.id] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	return retProducers
}

// 根据生产者id查找registration
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	//遍历registrationMap
	for k, producers := range r.registrationMap {
		//如果一个registration对应的producers中含有目标id
		if _, exists := producers[id]; exists {
			//将registration加入到结果集
			results = append(results, k)
		}
	}
	return results
}

// registration检查函数
func (k Registration) IsMatch(category string, key string, subkey string) bool {
	//如果registration类别不同，不匹配
	if category != k.Category {
		return false
	}
	//如果topic名称不为*且topic名称不同，不匹配
	if key != "*" && k.Key != key {
		return false
	}
	//如果channel名称不为*且channel名称不同，不匹配
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

// 根据指定registration过滤Registrations
func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

// 返回一个Registrations的key字符串切片（topic名称）
func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

// 返回一个Registrations的subkey字符串切片（channel名称）
func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

// 根据给定的生产者的存活时间和墓碑的存活时间来获取仍活着的生产者
func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		//获得生产者最后一次ping的时间
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		//如果生产者最大活跃时间内未响应或者已被标记为暂离状态，则跳过
		//如果距离上次ping的时间超过300秒（默认存活判别时间）则会被忽略
		//如果节点为游离态，则跳过
		if atomic.LoadInt64(&p.peerInfo.free) == 1 || now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		//否则加入到结果中
		results = append(results, p)
	}
	return results
}

// 返回producers的peerInfo切片
func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

// 将producermap变为producers切片
func ProducerMap2Slice(pm ProducerMap) Producers {
	//生成一个新的producer指针切片
	var producers Producers
	//遍历producermap，将所有producer加入到切片中
	for _, producer := range pm {
		producers = append(producers, producer)
	}
	//返回切片
	return producers
}

func (r *RegistrationDB) FindAllFreeNodes() Producers {
	//找到所有节点
	producers := r.FindProducers("client", "", "")
	var retproducers Producers
	for _, p := range producers {
		//如果该节点是游离态，则将其加入结果
		if atomic.LoadInt64(&p.peerInfo.free) == 1 {
			retproducers = append(retproducers, p)
		}
	}
	return retproducers
}

// 更新连接时长队列
func (fni *FreeNodeInfo) updateC(ConnectTime int64) {
	//队列未满时,头不动，尾增加
	if fni.cSize != 10 {
		fni.ConnectedInterval[fni.cRear] = float64(time.Now().Sub(time.Unix(0, ConnectTime))) / 1e9
		//如果有9个元素，即更新后队列刚好满，则队尾无需移动
		if fni.cSize != 9 {
			fni.cRear = (fni.cRear + 1) % 10
		}
		fni.cSize++
		return
	} else {
		//如果队列已满，则挤掉最早的数据
		fni.cFont = (fni.cFont + 1) % 10
		fni.cRear = (fni.cRear + 1) % 10
		fni.ConnectedInterval[fni.cRear] = float64(time.Now().Sub(time.Unix(0, ConnectTime))) / 1e9
		return
	}
}

// 更新断连时长队列
func (fni *FreeNodeInfo) updateR(lastTime int64) {
	//队列未满时,头不动，尾增加
	if fni.rSize != 10 {
		fni.ReconnectionInterval[fni.rRear] = float64(time.Now().Sub(time.Unix(0, lastTime))) / 1e9
		//如果有9个元素，即更新后队列刚好满，则队尾无需移动
		if fni.rSize != 9 {
			fni.rRear = (fni.rRear + 1) % 10
		}
		fni.rSize++
		return
	} else {
		//如果队列已满，则挤掉最早的数据
		fni.rFont = (fni.rFont + 1) % 10
		fni.rRear = (fni.rRear + 1) % 10
		fni.ReconnectionInterval[fni.rRear] = float64(time.Now().Sub(time.Unix(0, lastTime))) / 1e9
		return
	}
}

// 返回断连时间间隔的平均值
func (fni *FreeNodeInfo) getRIavage() float64 {

	var res float64 = 0
	t := fni.rFont
	for i := 0; i < fni.rSize; i++ {
		res = res + fni.ReconnectionInterval[t]
		t = (t + 1) % 10
	}
	return res / float64(fni.rSize)
}

// 返回断连时间间隔的方差
func (fni *FreeNodeInfo) getRIvariance() float64 {
	t := fni.rFont
	ava := fni.getRIavage()
	var res float64 = 0
	for i := 0; i < fni.rSize; i++ {
		res = res + math.Pow(fni.ReconnectionInterval[t]-ava, 2)
		t = (t + 1) % 10
	}
	return res / float64(fni.rSize)
}

// 返回连接时长的平均值
func (fni *FreeNodeInfo) getCIavage() float64 {
	var res float64 = 0
	t := fni.cFont
	for i := 0; i < fni.cSize; i++ {
		res = res + fni.ConnectedInterval[t]
		t = (t + 1) % 10
	}
	return res / float64(fni.rSize)
}
