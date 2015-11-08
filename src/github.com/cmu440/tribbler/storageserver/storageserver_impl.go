package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

// Value struct for single value(string type)
type sValue struct {
	value string
}

type lValue struct {
	value []string
}

type storageServer struct {
	hostport           string
	nodeId             uint32
	sMutex             *sync.Mutex // Mutex for single value map
	lMutex             *sync.Mutex // Mutex for list value map
	sMap               map[string]*sValue
	lMap               map[string]*lValue
	nodeIdMap          map[uint32]storagerpc.Node        // Map from node id to the node info (port, id)
	leaseMap           map[string](map[string]time.Time) // SUBJECT TO CHANGE
	keyLockMap         map[string]*sync.Mutex
	nodesList          []storagerpc.Node
	libStoreMap        map[string]*rpc.Client
	storageServerReady bool
	serverFull         chan int
	nodeSize           int
}

type nodes []storagerpc.Node

func (nodeList nodes) Len() int {
	return len(nodeList)
}

func (nodeList nodes) Less(i, j int) bool {
	return nodeList[i].NodeID < nodeList[j].NodeID
}

func (nodeList nodes) Swap(i, j int) {
	nodeList[i], nodeList[j] = nodeList[j], nodeList[i]
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	newss := &storageServer{
		hostport:           fmt.Sprintf("localhost:%d", port),
		nodeId:             nodeID,
		sMutex:             &sync.Mutex{},
		lMutex:             &sync.Mutex{},
		sMap:               make(map[string]*sValue),
		lMap:               make(map[string]*lValue),
		leaseMap:           make(map[string](map[string]time.Time)),
		keyLockMap:         make(map[string]*sync.Mutex),
		storageServerReady: false,
		libStoreMap:        make(map[string]*rpc.Client),
		nodeSize:           numNodes,
	}

	if len(masterServerHostPort) == 0 {
		// Master Storage Server to be created. First "join" itself.
		newss.nodeIdMap = make(map[uint32]storagerpc.Node)
		newss.nodesList = make([]storagerpc.Node, 0)
		thisNode := storagerpc.Node{newss.hostport, nodeID}
		newss.nodeIdMap[nodeID] = thisNode
		newss.nodesList = append(newss.nodesList, thisNode)
		newss.serverFull = make(chan int, 1)

		// RPC registration
		rpc.RegisterName("StorageServer", storagerpc.Wrap(newss))
		rpc.HandleHTTP()

		listener, err := net.Listen("tcp", newss.hostport)
		if err != nil {
			fmt.Println("Error:", err)
			return nil, errors.New("Listen error occurs.")
		}

		// Keep listening to connection requests
		go http.Serve(listener, nil)

		<-newss.serverFull
		newss.storageServerReady = true

	} else {
		// Slave Storage Server to be created. First "dial" the master.
		client, err2 := rpc.DialHTTP("tcp", masterServerHostPort)
		if err2 != nil {
			return nil, errors.New("")
		}

		registerArgs := &storagerpc.RegisterArgs{ServerInfo: storagerpc.Node{HostPort: fmt.Sprintf("localhost:%d", port), NodeID: nodeID}}
		var registerReply storagerpc.RegisterReply

		client.Call("StorageServer.RegisterServer", registerArgs, &registerReply)

		for registerReply.Status != storagerpc.OK {
			time.Sleep(time.Second)
			client.Call("StorageServer.RegisterServer", registerArgs, &registerReply)
		}

		newss.nodesList = registerReply.Servers

		rpc.RegisterName("StorageServer", storagerpc.Wrap(newss))
		rpc.HandleHTTP()

		listener, err3 := net.Listen("tcp", newss.hostport)
		if err3 != nil {
			fmt.Println("Error:", err3)
			return nil, errors.New("Listen error occurs.")
		}

		go http.Serve(listener, nil)
		newss.storageServerReady = true
	}

	tmp := nodes(newss.nodesList)
	sort.Sort(tmp)
	newss.nodesList = ([]storagerpc.Node)(tmp)
	go newss.CheckLease()
	return newss, nil
}

func (ss *storageServer) CheckLease() {
	expiration := time.Duration(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) * time.Second
	for {
		time.Sleep(500 * time.Millisecond)
		for k, hostTimeMap := range ss.leaseMap {
			for hp, tt := range hostTimeMap {
				if time.Since(tt) > expiration {
					delete(ss.leaseMap[k], hp)
				}
			}
		}
	}
}

func (ss *storageServer) CheckRevokeStatus(key string, successChan, finishChan chan int, expected int) {
	count := 0
	for {
		select {
		case <-successChan:
			count += 1
			lenMap := len(ss.leaseMap[key])
			if count == expected || lenMap == 0 {
				// Either all replied, or all expired
				finishChan <- 1
				return
			}
		case <-time.After(time.Second):
			if len(ss.leaseMap[key]) == 0 {
				finishChan <- 1
				return
			}
		}
	}
}

func (ss *storageServer) RevokeLeaseAt(hostport, key string, successChan chan int) {
	cli, ok := ss.libStoreMap[hostport]
	if !ok {
		var err error
		cli, err = rpc.DialHTTP("tcp", hostport)
		if err != nil {
			return
		}
		ss.libStoreMap[hostport] = cli
	}

	args := &storagerpc.RevokeLeaseArgs{key}
	var reply storagerpc.RevokeLeaseReply

	err2 := cli.Call("LeaseCallbacks", args, &reply)
	if err2 != nil {
		return
	}
	successChan <- 1
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	serverInfo := args.ServerInfo
	_, ok := ss.nodeIdMap[serverInfo.NodeID]
	if !ok {
		ss.nodeIdMap[serverInfo.NodeID] = args.ServerInfo
		ss.nodesList = append(ss.nodesList, args.ServerInfo)
	}

	if len(ss.nodeIdMap) < ss.nodeSize {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodesList
		ss.serverFull <- 1
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if ss.storageServerReady {
		reply.Status = storagerpc.OK
		tmp := make([]storagerpc.Node, len(ss.nodesList), cap(ss.nodesList))
		copy(tmp, ss.nodesList)
		reply.Servers = tmp
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if args == nil {
		return errors.New("ss: Can't get nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in Get")
	}
	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Step 1: Lock the keyLockMap access so as to find the lock for this specific key.
	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]
	if !exist {
		// Create new lock for the key
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	// Step 2: Release the sMutex lock so that the ss can serve other GET requests.
	// Meanwhile, since we are dealing with lease related to args.Key, we must lock
	// it using its own lock, keyLock.
	granted := false
	keyLock.Lock()
	defer keyLock.Unlock()

	if args.WantLease {
		leasedLibStores, ok := ss.leaseMap[args.Key]
		if !ok {
			leasedLibStores = make(map[string]time.Time)
			ss.leaseMap[args.Key] = leasedLibStores
		}
		ss.leaseMap[args.Key][args.HostPort] = time.Now()
		granted = true

	}
	reply.Lease = storagerpc.Lease{granted, storagerpc.LeaseSeconds}

	val, ok := ss.sMap[args.Key]
	if ok {
		reply.Value = val.value
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if args == nil {
		return errors.New("ss: Can't delete nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in Delete")
	}
	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]
	if !exist {
		// Create new lock for the key
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	keyLock.Lock()
	_, ok := ss.sMap[args.Key]

	if !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}

	hpTimeMap, leaseExists := ss.leaseMap[args.Key]
	if leaseExists {
		// Revoke all issued leases.
		successChan := make(chan int, 1)
		finishChan := make(chan int, 1)
		expected := len(hpTimeMap)
		go ss.CheckRevokeStatus(args.Key, successChan, finishChan, expected)
		for hp, _ := range hpTimeMap {
			go ss.RevokeLeaseAt(hp, args.Key, successChan)
		}

		<-finishChan

		delete(ss.leaseMap, args.Key)
	}

	delete(ss.sMap, args.Key)
	reply.Status = storagerpc.OK

	keyLock.Unlock()
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if args == nil {
		return errors.New("ss: Can't getList nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in GetList")
	}
	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]
	if !exist {
		// Create new lock for the key
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	granted := false
	keyLock.Lock()
	defer keyLock.Unlock()

	if args.WantLease {
		leasedLibStores, ok := ss.leaseMap[args.Key]
		if !ok {
			leasedLibStores = make(map[string]time.Time)
			ss.leaseMap[args.Key] = leasedLibStores
		}
		ss.leaseMap[args.Key][args.HostPort] = time.Now()
		granted = true

	}
	reply.Lease = storagerpc.Lease{granted, storagerpc.LeaseSeconds}

	lst, ok := ss.lMap[args.Key]
	if ok {
		// Copy and return the current list
		rst := make([]string, len(lst.value), cap(lst.value))
		copy(rst, lst.value)
		reply.Value = rst
		reply.Status = storagerpc.OK
	} else {
		reply.Value = make([]string, 0, 0)
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if args == nil {
		return errors.New("ss: Can't put nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in Put")
	}
	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]
	if !exist {
		// Create new lock for the key
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	keyLock.Lock()
	_, ok := ss.sMap[args.Key]

	if !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}

	hpTimeMap, leaseExists := ss.leaseMap[args.Key]
	if leaseExists {
		// Revoke all issued leases.
		successChan := make(chan int, 1)
		finishChan := make(chan int, 1)
		expected := len(ss.leaseMap[args.Key])
		go ss.CheckRevokeStatus(args.Key, successChan, finishChan, expected)
		for hp, _ := range hpTimeMap {
			go ss.RevokeLeaseAt(hp, args.Key, successChan)
		}

		<-finishChan

		delete(ss.leaseMap, args.Key)
	}

	newValue := sValue{
		value: args.Value,
	}
	ss.sMap[args.Key] = &newValue
	reply.Status = storagerpc.OK

	keyLock.Unlock()
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if args == nil {
		return errors.New("ss: Can't append nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in Append")
	}
	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]
	if !exist {
		// Create new lock for the key
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	keyLock.Lock()
	hpTimeMap, leaseExists := ss.leaseMap[args.Key]
	if leaseExists {
		// Revoke all issued leases.
		successChan := make(chan int, 1)
		finishChan := make(chan int, 1)
		expected := len(ss.leaseMap[args.Key])
		go ss.CheckRevokeStatus(args.Key, successChan, finishChan, expected)
		for hp, _ := range hpTimeMap {
			go ss.RevokeLeaseAt(hp, args.Key, successChan)
		}

		<-finishChan

		delete(ss.leaseMap, args.Key)
	}

	lst, ok := ss.lMap[args.Key]
	if ok {
		for _, v := range lst.value {
			if v == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
		lst.value = append(lst.value, args.Value)
	} else {
		newValue := lValue{
			value: make([]string, 1),
		}
		newValue.value[0] = args.Value
		ss.lMap[args.Key] = &newValue
	}
	reply.Status = storagerpc.OK

	keyLock.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if args == nil {
		return errors.New("ss: Can't Reomove nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in Remove")
	}
	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]
	if !exist {
		// Create new lock for the key
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	keyLock.Lock()

	hpTimeMap, leaseExists := ss.leaseMap[args.Key]
	if leaseExists {
		// Revoke all issued leases.
		successChan := make(chan int, 1)
		finishChan := make(chan int, 1)
		expected := len(ss.leaseMap[args.Key])
		go ss.CheckRevokeStatus(args.Key, successChan, finishChan, expected)
		for hp, _ := range hpTimeMap {
			go ss.RevokeLeaseAt(hp, args.Key, successChan)
		}

		<-finishChan

		delete(ss.leaseMap, args.Key)
	}

	lst, ok := ss.lMap[args.Key]
	if ok {
		for i, v := range lst.value {
			if v == args.Value {
				lst.value = append(lst.value[:i], lst.value[i+1:]...)
				reply.Status = storagerpc.OK
				return nil
			}
		}
	}
	reply.Status = storagerpc.ItemNotFound

	keyLock.Unlock()
	return nil
}

func (ss *storageServer) CheckKeyInRange(key string) bool {
	hashedValue := libstore.StoreHash(key)
	serverId := 0

	for i := 0; i < len(ss.nodesList); i++ {
		if i == len(ss.nodesList)-1 {
			serverId = 0
			break
		}
		if hashedValue > ss.nodesList[i].NodeID && hashedValue <= ss.nodesList[i+1].NodeID {
			serverId = i + 1
			break
		}
	}

	return uint32(serverId) == ss.nodeId
}
