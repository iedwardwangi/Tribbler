package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type sCachNode struct {
	value   string
	seconds int
	isValid bool
}

type lCachNode struct {
	value   []string
	seconds int
	isValid bool
}

type libstore struct {
	// TODO: extends to many servers
	master         *rpc.Client
	svrMap         map[uint32]*rpc.Client // map from NodeID to rpc.Client (which we can call)
	svrMutex       *sync.Mutex
	allServerNodes []storagerpc.Node
	myHostPort     string
	mode           LeaseMode

	// Cache
	counter      map[string]int
	counterMutex *sync.Mutex
	sCache       map[string]*sCachNode
	sMutex       *sync.Mutex
	lCache       map[string]*lCachNode
	lMutex       *sync.Mutex
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

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	master, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply

	for i := 0; i < 5; i++ {
		master.Call("StorageServer.GetServers", args, &reply)
		if reply.Status == storagerpc.OK {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	if reply.Status != storagerpc.OK {
		return nil, nil // After retry for 5 times...
	}

	nodeList := nodes(reply.Servers)
	sort.Sort(nodeList)
	nodeListSorted := ([]storagerpc.Node)(nodeList)

	newLib := &libstore{
		master:         master,
		svrMap:         make(map[uint32]*rpc.Client),
		myHostPort:     myHostPort,
		allServerNodes: nodeListSorted,
		svrMutex:       &sync.Mutex{},
		counter:        make(map[string]int),
		counterMutex:   &sync.Mutex{},
		sMutex:         &sync.Mutex{},
		sCache:         make(map[string]*sCachNode),
		lMutex:         &sync.Mutex{},
		lCache:         make(map[string]*lCachNode),
		mode:           mode,
	}

	if mode != Never {
		rpc.RegisterName("LeaseCallbacks", librpc.Wrap(newLib))
	}
	go newLib.selfRevoke()
	go newLib.selfCleanCounter()

	return newLib, nil
}

func (ls *libstore) selfCleanCounter() {
	for {
		time.Sleep(storagerpc.QueryCacheSeconds * time.Second)
		ls.counterMutex.Lock()
		for k, _ := range ls.counter {
			ls.counter[k] = 0
		}
		ls.counterMutex.Unlock()
	}
}

func (ls *libstore) selfRevoke() {
	for {
		time.Sleep(time.Second)
		ls.sMutex.Lock()
		for k, v := range ls.sCache {
			if v.isValid {
				v.seconds = v.seconds - 1
				if v.seconds <= 0 {
					//delete
					delete(ls.sCache, k)
				}
			}
		}
		ls.sMutex.Unlock()

		ls.lMutex.Lock()
		for k, v := range ls.lCache {
			if v.isValid {
				v.seconds = v.seconds - 1
				if v.seconds <= 0 {
					//delete
					delete(ls.lCache, k)
				}
			}
		}
		ls.lMutex.Unlock()
	}
}

func (ls *libstore) Get(key string) (string, error) {
	// Check for cache
	ls.sMutex.Lock()
	node, ok := ls.sCache[key]
	if ok {
		if node.isValid {
			rst := node.value
			ls.sMutex.Unlock()
			return rst, nil
		}
	}
	ls.sMutex.Unlock()

	// Cache not found, need to increment counter
	ls.counterMutex.Lock()
	n, ok := ls.counter[key]
	if ok {
		ls.counter[key] = n + 1
	} else {
		ls.counter[key] = 1
	}
	ls.counterMutex.Unlock()

	var args *storagerpc.GetArgs
	// TODO: figure out to use n or n-1
	if ok && n >= storagerpc.QueryCacheThresh {
		args = &storagerpc.GetArgs{key, true, ls.myHostPort}
	} else {
		args = &storagerpc.GetArgs{key, false, ls.myHostPort}
	}

	if ls.mode == Always {
		args = &storagerpc.GetArgs{key, true, ls.myHostPort}
	}
	if ls.mode == Never {
		args = &storagerpc.GetArgs{key, false, ls.myHostPort}
	}

	var reply storagerpc.GetReply

	cli, err := ls.GetStorageServer(key)
	if err != nil {
		return "", err
	}

	err2 := cli.Call("StorageServer.Get", args, &reply)
	if err2 != nil {
		return "", err2
	}

	if reply.Status != storagerpc.OK {
		return "", errors.New("Status not OK.")
	}

	if args.WantLease && reply.Lease.Granted {
		ls.sMutex.Lock()
		ls.sCache[key] = &sCachNode{
			value:   reply.Value,
			seconds: reply.Lease.ValidSeconds,
			isValid: true,
		}
		ls.sMutex.Unlock()
	}

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{key, value}
	var reply storagerpc.PutReply

	cli, err := ls.GetStorageServer(key)
	if err != nil {
		return err
	}

	err = cli.Call("StorageServer.Put", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK.")
	}

	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{key}
	var reply storagerpc.DeleteReply

	cli, err := ls.GetStorageServer(key)
	if err != nil {
		return err
	}

	err = cli.Call("StorageServer.Delete", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK.")
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// Check for cache
	ls.lMutex.Lock()
	node, ok := ls.lCache[key]
	if ok {
		if node.isValid {
			rst := node.value
			ls.lMutex.Unlock()
			return rst, nil
		}
	}
	ls.lMutex.Unlock()

	// Cache not found, need to increment counter
	ls.counterMutex.Lock()
	n, ok := ls.counter[key]
	if ok {
		ls.counter[key] = n + 1
	} else {
		ls.counter[key] = 1
	}
	ls.counterMutex.Unlock()

	var args *storagerpc.GetArgs
	// TODO: figure out to use n or n-1
	if ok && n >= storagerpc.QueryCacheThresh {
		args = &storagerpc.GetArgs{key, true, ls.myHostPort}
	} else {
		args = &storagerpc.GetArgs{key, false, ls.myHostPort}
	}

	if ls.mode == Always {
		args = &storagerpc.GetArgs{key, true, ls.myHostPort}
	}
	if ls.mode == Never {
		args = &storagerpc.GetArgs{key, false, ls.myHostPort}
	}

	var reply storagerpc.GetListReply

	cli, err := ls.GetStorageServer(key)
	if err != nil {
		return nil, err
	}

	err = cli.Call("StorageServer.GetList", args, &reply)
	if err != nil {
		return nil, err
	}

	if reply.Status != storagerpc.OK {
		return nil, errors.New("Status not OK.")
	}

	if args.WantLease && reply.Lease.Granted {
		ls.lMutex.Lock()
		ls.lCache[key] = &lCachNode{
			value:   reply.Value,
			seconds: reply.Lease.ValidSeconds,
			isValid: true,
		}
		ls.lMutex.Unlock()
	}

	return reply.Value, nil
}

/*
 * GetStorageServer: Givern a key (string), return the rpc.Client that will be used
 * to call the corresponding remote storage server.
 */
func (ls *libstore) GetStorageServer(key string) (*rpc.Client, error) {
	// TODO: that as well.
	hashedKeyValue := StoreHash(key)
	ssNode := ls.allServerNodes[ls.GetStorageServerId(hashedKeyValue)]

	svr, ok := ls.svrMap[ssNode.NodeID]
	if !ok {
		// Use as write lock only
		ls.svrMutex.Lock()
		defer ls.svrMutex.Unlock()

		svr, err := rpc.DialHTTP("tcp", ssNode.HostPort)
		if err != nil {
			fmt.Println("Location 6:", err)
			return nil, err
		}

		ls.svrMap[ssNode.NodeID] = svr
		return svr, nil
	}
	return svr, nil
}

func (ls *libstore) GetStorageServerId(hashedValue uint32) uint32 {
	serverId := 0

	// Note: ls.allServerNodes is SORTED.
	for i := 0; i < len(ls.allServerNodes); i++ {
		if i == len(ls.allServerNodes)-1 {
			return 0
		}

		if hashedValue > ls.allServerNodes[i].NodeID && hashedValue <= ls.allServerNodes[i+1].NodeID {
			serverId = i + 1
			break
		}

	}
	return uint32(serverId)
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{key, removeItem}
	var reply storagerpc.PutReply

	cli, err := ls.GetStorageServer(key)
	if err != nil {
		return err
	}
	err = cli.Call("StorageServer.RemoveFromList", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK.")
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{key, newItem}
	var reply storagerpc.PutReply

	cli, err := ls.GetStorageServer(key)
	if err != nil {
		return err
	}

	err = cli.Call("StorageServer.AppendToList", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK.")
	}

	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.sMutex.Lock()
	_, ok := ls.sCache[args.Key]
	if ok {
		delete(ls.sCache, args.Key)
		ls.sMutex.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}
	ls.sMutex.Unlock()

	ls.lMutex.Lock()
	_, ok = ls.lCache[args.Key]
	if ok {
		delete(ls.lCache, args.Key)
		ls.lMutex.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}
	ls.lMutex.Unlock()

	// Both not found
	reply.Status = storagerpc.KeyNotFound
	return nil
}
