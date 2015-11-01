package storageserver

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"sync"
)

// Value struct for single value(string type)
type sValue struct {
	value string
}

type lValue struct {
	value []string
}

type storageServer struct {
	sMutex sync.Mutex // Mutex for single value map
	lMutex sync.Mutex // Mutex for list value map
	sMap   map[string]*sValue
	lMap   map[string]*lValue
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
	return nil, errors.New("not implemented")
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
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
		reply.Status = OK
	} else {
		reply.Status = KeyNotFound
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

	val, ok := ss.sMap[args.Key]
	if ok {
		delete(ss.sMap[args.Key])
		reply.Status = OK
	} else {
		reply.Status = KeyNotFound
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	return errors.New("not implemented")
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
	reply.Status = OK
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

	} else {
		newValue := lValue{
			value := make([]string, 1)
		}
		
		ss.lMap[args.Key] = 
	}
	newValue := sValue{
		value: args.Value,
	}
	ss.sMap[args.Key] = &newValue
	reply.Status = OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}
