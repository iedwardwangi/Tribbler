package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
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
	nodeIdMap          map[uint32]storagerpc.Node // Map from node id to the node info (port, id)
	nodesList          []storagerpc.Node
	storageServerReady bool
	serverFull         chan int
	nodeSize           int
}

func PrintError(s string) {
	fmt.Println("Error:", s)
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
		storageServerReady: false,
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
		return newss, nil
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
		return newss, nil

	}
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
	ss.sMutex.Lock()
	defer ss.sMutex.Unlock()

	// TODO: check key in range for the server

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
	ss.sMutex.Lock()
	defer ss.sMutex.Unlock()

	// TODO: check key in range for the server

	_, ok := ss.sMap[args.Key]
	if ok {
		delete(ss.sMap, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if args == nil {
		return errors.New("ss: Can't getList nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in GetList")
	}
	ss.lMutex.Lock()
	defer ss.lMutex.Unlock()

	// TODO: check key in range for the server

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
	defer ss.sMutex.Unlock()

	// TODO: check key in range for the server

	newValue := sValue{
		value: args.Value,
	}
	ss.sMap[args.Key] = &newValue
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if args == nil {
		return errors.New("ss: Can't append nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in Append")
	}
	ss.lMutex.Lock()
	defer ss.lMutex.Unlock()

	// TODO: check key in range for the server

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
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if args == nil {
		return errors.New("ss: Can't Reomove nil K/V pair")
	}
	if reply == nil {
		return errors.New("ss: Can't reply with nil in Remove")
	}
	ss.lMutex.Lock()
	defer ss.lMutex.Unlock()

	// TODO: check key in range for the server

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

	return nil
}
