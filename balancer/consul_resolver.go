package balancer

import (
	"math"
	"strconv"
	"time"

	"github.com/hashicorp/consul/api"
	jsoniter "github.com/json-iterator/go"
	"github.com/mae-pax/consul-loadbalancer/util"
)

const (
	BALANCEFACTOR_MAX_LOCAL   = 3000
	BALANCEFACTOR_MIN_LOCAL   = 200
	BALANCEFACTOR_MAX_CROSS   = 1000
	BALANCEFACTOR_MIN_CROSS   = 1
	BALANCEFACTOR_START_CROSS = 50
	BALANCEFACTOR_CROSS_RATE  = 0.1
)

type ConsulResolverBuilder struct {
	Cloud             string
	Address           string
	Service           string
	CPUThresholdKey   string
	ZoneCPUKey        string
	InstanceFactorKey string
	OnlineLabKey      string
	Interval          time.Duration
	Timeout           time.Duration
}

func (b *ConsulResolverBuilder) Build() (*ConsulResolver, error) {
	return NewConsulResolver(b.Cloud, b.Address, b.Service, b.CPUThresholdKey, b.ZoneCPUKey, b.InstanceFactorKey, b.OnlineLabKey, b.Interval, b.Timeout)
}

func NewConsulResolver(cloud, address, service, cpuThresholdKey, zoneCPUKey, instanceFactorKey, onlineLabKey string, interval, timeout time.Duration, args ...string) (*ConsulResolver, error) {
	config := api.DefaultConfig()
	config.Address = address
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}

	r := &ConsulResolver{
		client:             client,
		address:            address,
		service:            service,
		interval:           interval,
		timeout:            timeout,
		cpuThresholdKey:    cpuThresholdKey,
		zoneCPUKey:         zoneCPUKey,
		instanceFactorKey:  instanceFactorKey,
		onlineLabKey:       onlineLabKey,
		zone:               util.Zone(cloud),
		done:               make(chan bool),
		balanceFactorCache: make(map[string]float64),
	}
	if len(args) != 0 {
		r.k8sServiceKey = args[0]
	}

	return r, nil
}

type ConsulResolver struct {
	client             *api.Client
	address            string
	service            string
	lastIndex          uint64
	zone               string
	candidatePool      *CandidatePool
	localZone          *ServiceZone
	serviceZones       []*ServiceZone
	zoneCPUMap         map[string]float64
	instanceFactorMap  map[string]float64
	balanceFactorCache map[string]float64
	interval           time.Duration
	timeout            time.Duration
	done               chan bool
	cpuThreshold       float64
	onlineLab          *OnlineLab
	k8sServiceKey      string
	cpuThresholdKey    string
	instanceFactorKey  string
	onlineLabKey       string
	zoneCPUKey         string
	metric             *ConsulResolverMetric
	zoneCPUUpdated     bool
	logger             util.Logger
	watcherLogger      util.Logger
	watcher            *util.Watch
}

type ConsulResolverMetric struct {
	candidatePoolSize int
	crossZoneNum      int
	selectNum         int
}

type OnlineLab struct {
	CrossZone         bool    `json:"crossZone"`
	CrossZoneRate     float64 `json:"crossZoneRate"` // TODO
	FactorCacheExpire int     `json:"factorCacheExpire"`
	FactorStartRate   float64 `json:"factorStartRate"`
	LearningRate      float64 `json:"learningRate"`
	RateThreshold     float64 `json:"rateThreshold"`
}

type CandidatePool struct {
	Nodes     []*ServiceNode
	Factors   []float64
	Weights   []float64
	FactorSum float64
}

type ServiceNodes struct {
	UpdateTime int64
	Data       []ServiceNode
}

type ServiceNode struct {
	PublicIP      string
	InstanceID    string
	Host          string
	Port          int
	Zone          string
	BalanceFactor float64
	CurrentFactor float64
	WorkLoad      float64
}

type ServiceZone struct {
	Nodes    []*ServiceNode
	Zone     string
	WorkLoad float64
}

type CPUThreshold struct {
	CThreshold float64 `json:"cpuThreshold"`
}

type ZoneCPUUtilizationRatio struct {
	Updated int64                `json:"updated"`
	Date    []map[string]float64 `json:"data"`
}

type InstanceFactor struct {
	Updated int64              `json:"updated"`
	Date    []InstanceMetaInfo `json:"data"`
}

type InstanceMetaInfo struct {
	PublicIP       string  `json:"public_ip"`
	InstanceID     string  `json:"instanceid"`
	CPUUtilization float64 `json:"CPUUtilization"`
	Zone           string  `json:"zone"`
}

func (r *ConsulResolver) SetLogger(logger util.Logger) {
	r.logger = logger
}

func (r *ConsulResolver) SetWatcher(watcherLogger util.Logger) {
	r.watcherLogger = watcherLogger
}

func (r *ConsulResolver) SetZone(zone string) {
	r.zone = zone
}

func (r *ConsulResolver) Start() error {
	if err := r.updateAll(); err != nil {
		return err
	}

	if r.logger != nil {
		r.logger.Infof("new consul resolver start. [%#v]", r)
	}

	if r.watcherLogger != nil {
		r.watcher = util.NewWatch(r.watcherLogger)
		r.watcher.RunWatch()
	}

	go func() {
		tk := time.NewTicker(r.interval)
		for {
			select {
			case <-tk.C:
				if err := r.updateAll(); err != nil {
					r.logger.Warnf("updateAll failed. err: %s", err.Error())
				}
			case <-r.done:
				r.logger.Infof("consul resolver get stop signal, will stop")
				tk.Stop()
				return
			}
		}
	}()

	return nil
}

func (r *ConsulResolver) Stop() {
	r.done <- true
	if r.watcherLogger != nil {
		r.watcher.Stop()
	}
}

func (r *ConsulResolver) updateAll() error {
	r.logger.Debugf("======== start updateAll ========")
	err := r.updateCPUThreshold()
	if err != nil {
		return err
	}
	err = r.updateZoneCPUMap()
	if err != nil {
		return err
	}
	err = r.updateOnlineLabFactor()
	if err != nil {
		return err
	}
	err = r.updateInstanceFactorMap()
	if err != nil {
		return err
	}
	err = r.updateServiceZone()
	if err != nil {
		return err
	}
	r.expireBalanceFactorCache()
	r.updateCandidatePool()
	r.logger.Debugf("======== end updateAll ========")
	return nil
}

func (r *ConsulResolver) updateCPUThreshold() error {
	res, _, err := r.client.KV().Get(r.cpuThresholdKey, nil)
	if err != nil {
		return err
	}
	var ct CPUThreshold
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(res.Value, &ct)
	if err != nil {
		return err
	}
	r.cpuThreshold = ct.CThreshold
	r.logger.Debugf("update cpuThreshold: %f, key: %s", r.cpuThreshold, r.cpuThresholdKey)
	return nil
}

func (r *ConsulResolver) updateZoneCPUMap() error {
	res, _, err := r.client.KV().Get(r.zoneCPUKey, nil)
	if err != nil {
		return err
	}
	var zc ZoneCPUUtilizationRatio
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(res.Value, &zc)
	if err != nil {
		return err
	}
	if time.Now().Unix()-zc.Updated < 300 {
		r.zoneCPUUpdated = true
	} else {
		r.zoneCPUUpdated = false
		r.logger.Warnf("%s no update, will hold factor learning", r.zoneCPUKey)
	}
	m := make(map[string]float64)
	for _, v := range zc.Date {
		for k, vv := range v {
			m[k] = vv
		}
	}
	r.zoneCPUMap = m
	r.logger.Debugf("update zoneCPUMap: %+v, key: %s", r.zoneCPUMap, r.zoneCPUKey)
	return nil
}

func (r *ConsulResolver) updateOnlineLabFactor() error {
	res, _, err := r.client.KV().Get(r.onlineLabKey, nil)
	if err != nil {
		return err
	}
	var ol OnlineLab
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(res.Value, &ol)
	if err != nil {
		return err
	}
	r.onlineLab = &ol
	r.logger.Debugf("update onlineLab: %+v, key: %s", r.onlineLab, r.onlineLabKey)
	return nil
}

func (r *ConsulResolver) updateInstanceFactorMap() error {
	res, _, err := r.client.KV().Get(r.instanceFactorKey, nil)
	if err != nil {
		return err
	}
	var i InstanceFactor
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(res.Value, &i)
	if err != nil {
		return err
	}
	m := make(map[string]float64)
	for _, v := range i.Date {
		m[v.InstanceID] = v.CPUUtilization
	}
	r.instanceFactorMap = m
	r.logger.Debugf("update instanceFactorMap: %+v, key: %s", r.instanceFactorMap, r.instanceFactorKey)
	return nil
}

func (r *ConsulResolver) updateServiceZone() error {
	var serviceNodes []ServiceNode
	if r.k8sServiceKey != "" {
		var services ServiceNodes
		res, _, err := r.client.KV().Get(r.k8sServiceKey, nil)
		if err != nil {
			return err
		}
		err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(res.Value, &services)
		if err != nil {
			return err
		}
		serviceNodes = services.Data
	} else {
		qm := api.QueryOptions{}
		qm.WaitIndex = r.lastIndex
		qm.WaitTime = r.timeout
		res, meta, err := r.client.Health().Service(r.service, "", true, &qm)
		if err != nil {
			return err
		}
		r.lastIndex = meta.LastIndex
		serviceNodes = make([]ServiceNode, len(res))
		for i, entry := range res {
			serviceNode := ServiceNode{}
			serviceNode.Zone = entry.Service.Meta["zone"]
			balanceFactor, _ := strconv.ParseFloat(entry.Service.Meta["balanceFactor"], 64)
			serviceNode.BalanceFactor = balanceFactor
			serviceNode.InstanceID = entry.Service.Meta["instanceID"]
			serviceNode.PublicIP = entry.Service.Meta["publicIP"]
			serviceNode.Host = entry.Service.Address
			serviceNode.Port = entry.Service.Port
			serviceNodes[i] = serviceNode
		}
	}

	m := make(map[string]*ServiceZone)
	for _, v := range serviceNodes {
		workload, ok := r.instanceFactorMap[v.InstanceID]
		if !ok {
			v.WorkLoad = 100
		} else {
			v.WorkLoad = workload
		}

		sz, ok := m[v.Zone]
		if !ok {
			z := &ServiceZone{
				Zone:     v.Zone,
				WorkLoad: 100,
				Nodes:    make([]*ServiceNode, 0),
			}
			if zoneWorkload, ok := r.zoneCPUMap[v.Zone]; ok {
				z.WorkLoad = zoneWorkload
			}
			node := ServiceNode{}
			node = v
			z.Nodes = append(z.Nodes, &node)
			m[v.Zone] = z
			r.logger.Debugf("service: %s, zone: %s, workload: %f, node: %+v", r.service, v.Zone, z.WorkLoad, v)
		} else {
			node := ServiceNode{}
			node = v
			sz.Nodes = append(sz.Nodes, &node)
			r.logger.Debugf("service: %s, zone: %s, workload: %f, node: %+v", r.service, v.Zone, sz.WorkLoad, v)
		}
	}

	serviceZones := make([]*ServiceZone, 0)
	for _, v := range m {
		serviceZones = append(serviceZones, v)
		if v.Zone == r.zone {
			r.localZone = v
		}
	}
	r.serviceZones = serviceZones
	return nil
}

func (r *ConsulResolver) expireBalanceFactorCache() {
	if 1 == util.IntPseudoRandom(1, r.onlineLab.FactorCacheExpire) {
		r.balanceFactorCache = make(map[string]float64)
		r.logger.Debugf("remove balanceFactorCache")
	}
}

func (r *ConsulResolver) updateCandidatePool() {
	localZone := r.localZone
	serviceZones := r.serviceZones
	balanceFactorCache := r.balanceFactorCache
	candidatePool := new(CandidatePool)
	var factorCached bool
	if len(r.balanceFactorCache) > 0 {
		factorCached = true
	}
	var localAvgFactor float64

	for _, serviceZone := range serviceZones {
		if (r.localZone == nil && r.onlineLab.CrossZone) || r.localZone.Zone == serviceZone.Zone {
			r.logger.Debugf("current zone: %s, %s", r.zone, serviceZone.Zone)
			for _, node := range serviceZone.Nodes {
				candidatePool.Nodes = append(candidatePool.Nodes, node)
				candidatePool.Weights = append(candidatePool.Weights, 0)
				balanceFactor := node.BalanceFactor
				if factorCached {
					bf, ok := balanceFactorCache[node.InstanceID]
					if ok {
						balanceFactor = bf
						r.logger.Debugf("balanceFactor update, factorCached balanceFactor: %f", balanceFactor)
					} else if localAvgFactor > 0 {
						balanceFactor = localAvgFactor
						r.logger.Debugf("balanceFactor update, localAvgFactor balanceFactor: %f", balanceFactor)
					} else {
						balanceFactor = node.BalanceFactor * r.onlineLab.FactorStartRate
						r.logger.Debugf("balanceFactor update, node.BalanceFactor * r.onlineLab.FactorStartRate balanceFactor: %f", balanceFactor)
					}
				}
				r.logger.Debugf("will check nodeBalance, node.WorkLoad: %f, serviceZone.WorkLoad: %f, r.onlineLab.RateThreshold: %f, r.zoneCPUUpdated: %t",
					node.WorkLoad, serviceZone.WorkLoad, r.onlineLab.RateThreshold, r.zoneCPUUpdated)

				if !r.nodeBalanced(node, serviceZone) && r.zoneCPUUpdated {
					if node.WorkLoad > serviceZone.WorkLoad {
						balanceFactor -= balanceFactor * r.onlineLab.LearningRate
						r.logger.Debugf("balanceFactor update, balanceFactor -= balanceFactor * r.onlineLab.LearningRate: %f", balanceFactor)
					} else {
						balanceFactor += balanceFactor * r.onlineLab.LearningRate
						r.logger.Debugf("balanceFactor update, balanceFactor += balanceFactor * r.onlineLab.LearningRate: %f", balanceFactor)
					}
				}
				if balanceFactor > BALANCEFACTOR_MAX_LOCAL {
					balanceFactor = BALANCEFACTOR_MAX_LOCAL
					r.logger.Debugf("balanceFactor update, BALANCEFACTOR_MAX_LOCAL: %f", balanceFactor)
				} else if balanceFactor < BALANCEFACTOR_MIN_LOCAL {
					balanceFactor = BALANCEFACTOR_MIN_LOCAL
					r.logger.Debugf("balanceFactor update, BALANCEFACTOR_MIN_LOCAL: %f", balanceFactor)
				}
				// r.logger.Infof("balanceFactor: %f", balanceFactor)
				node.CurrentFactor = balanceFactor
				candidatePool.Factors = append(candidatePool.Factors, balanceFactor)
				candidatePool.FactorSum += balanceFactor
				balanceFactorCache[node.InstanceID] = balanceFactor
				r.logger.Debugf("balanceFactorCache: %+v", balanceFactorCache)
			}
			if len(candidatePool.Factors) > 0 {
				localAvgFactor = candidatePool.FactorSum / float64(len(candidatePool.Factors))
				r.logger.Debugf("localAvgFactor updated: %f", localAvgFactor)
			}
		} else if r.onlineLab.CrossZone && r.zoneCPUMap[r.localZone.Zone] > r.cpuThreshold && r.onlineLab.CrossZoneRate > util.FloatPseudoRandom() {
			r.logger.Debugf("when crossZone is true, current zone: %s, %s", r.zone, serviceZone.Zone)
			for _, node := range serviceZone.Nodes {
				candidatePool.Nodes = append(candidatePool.Nodes, node)
				candidatePool.Weights = append(candidatePool.Weights, 0)
				balanceFactor := node.BalanceFactor
				bf, ok := balanceFactorCache[node.InstanceID]
				if ok {
					balanceFactor = bf
					r.logger.Debugf("balanceFactor update, factorCached balanceFactor: %f", balanceFactor)
				}
				if !r.zoneBalanced(localZone, serviceZone) && localZone.WorkLoad > r.cpuThreshold && localZone.WorkLoad > serviceZone.WorkLoad {
					balanceFactor = balanceFactor * BALANCEFACTOR_CROSS_RATE
					r.logger.Debugf("balanceFactor update, balanceFactor = balanceFactor * BALANCEFACTOR_CROSS_RATE: %f", balanceFactor)
				} else {
					// balanceFactor = balanceFactor * (localZone.WorkLoad - serviceZone.WorkLoad) / 100.0
					balanceFactor = BALANCEFACTOR_MIN_CROSS
					r.logger.Debugf("balanceFactor update, balanceFactor = BALANCEFACTOR_MIN_CROSS: %f", balanceFactor)
				}
				if r.zoneCPUUpdated {
					if !r.zoneBalanced(localZone, serviceZone) && localZone.WorkLoad > r.cpuThreshold && localZone.WorkLoad > serviceZone.WorkLoad {
						if balanceFactor < BALANCEFACTOR_START_CROSS {
							balanceFactor = BALANCEFACTOR_START_CROSS
							r.logger.Debugf("balanceFactor update, balanceFactor = BALANCEFACTOR_START_CROSS: %f", balanceFactor)
						}
						balanceFactor += balanceFactor * r.onlineLab.LearningRate
						r.logger.Debugf("balanceFactor update, balanceFactor += balanceFactor * r.onlineLab.LearningRate: %f", balanceFactor)
					} else {
						balanceFactor -= balanceFactor * r.onlineLab.LearningRate
						r.logger.Debugf("balanceFactor update, balanceFactor -= balanceFactor * r.onlineLab.LearningRate: %f", balanceFactor)
					}
					if !r.nodeBalanced(node, serviceZone) {
						if node.WorkLoad > serviceZone.WorkLoad {
							balanceFactor += balanceFactor * r.onlineLab.LearningRate
							r.logger.Debugf("balanceFactor update, balanceFactor += balanceFactor * r.onlineLab.LearningRate: %f", balanceFactor)
						} else {
							balanceFactor -= balanceFactor * r.onlineLab.LearningRate
							r.logger.Debugf("balanceFactor update, balanceFactor -= balanceFactor * r.onlineLab.LearningRate: %f", balanceFactor)
						}
					}
				}
				if balanceFactor > BALANCEFACTOR_MAX_CROSS {
					balanceFactor = BALANCEFACTOR_MAX_CROSS
					r.logger.Debugf("balanceFactor update, BALANCEFACTOR_MAX_CROSS: %f", balanceFactor)
				} else if balanceFactor < BALANCEFACTOR_MIN_CROSS {
					balanceFactor = BALANCEFACTOR_MIN_CROSS
					r.logger.Debugf("balanceFactor update, BALANCEFACTOR_MIN_CROSS: %f", balanceFactor)
				}
				// r.logger.Infof("balanceFactor: %f", balanceFactor)
				node.CurrentFactor = balanceFactor
				candidatePool.Factors = append(candidatePool.Factors, balanceFactor)
				candidatePool.FactorSum += balanceFactor
				balanceFactorCache[node.InstanceID] = balanceFactor
				r.logger.Debugf("balanceFactorCache: %+v", balanceFactorCache)
			}
		}
	}

	candidatePoolSize := len(candidatePool.Nodes)
	if r.metric != nil {
		r.metric.candidatePoolSize = candidatePoolSize
	} else {
		cm := ConsulResolverMetric{}
		cm.candidatePoolSize = candidatePoolSize
		r.metric = &cm
		r.logger.Debugf("init metric: %+v", r.metric)
	}

	r.candidatePool = candidatePool
	return
}

func (r *ConsulResolver) nodeBalanced(node *ServiceNode, zone *ServiceZone) bool {
	return math.Abs(node.WorkLoad-zone.WorkLoad)/100.0 < r.onlineLab.RateThreshold
}

func (r *ConsulResolver) zoneBalanced(localZone *ServiceZone, crossZone *ServiceZone) bool {
	return math.Abs(localZone.WorkLoad-crossZone.WorkLoad)/100.0 < r.onlineLab.RateThreshold*2
}

func (r *ConsulResolver) SelectNode() *ServiceNode {
	if len(r.candidatePool.Nodes) == 0 {
		return nil
	}

	var idx int
	var max float64
	for i := 0; i < len(r.candidatePool.Factors); i++ {
		r.candidatePool.Weights[i] += r.candidatePool.Factors[i]
		if max < r.candidatePool.Weights[i] {
			max = r.candidatePool.Weights[i]
			idx = i
		}
	}
	r.logger.Debugf("index: %d", idx)
	node := r.candidatePool.Nodes[idx]
	r.logger.Debugf("select node: %+v", node)
	r.candidatePool.Weights[idx] -= r.candidatePool.FactorSum
	r.metric.selectNum += 1

	if node.Zone != r.zone {
		r.metric.crossZoneNum += 1
	}

	r.logger.Debugf("metric: %+v", r.metric)
	if r.watcher != nil && r.watcherLogger != nil {
		r.watcher.AddWatchValue(node.Host, 1)
		r.watcher.AddAvgWatchValue(node.Host+"_workload", node.WorkLoad)
	}
	return node
}

func (r *ConsulResolver) GetZoneNodes(zone string) []*ServiceNode {
	var nodes []*ServiceNode
	for _, serviceZone := range r.serviceZones {
		if zone == serviceZone.Zone {
			nodes = serviceZone.Nodes
		}
	}
	return nodes
}
