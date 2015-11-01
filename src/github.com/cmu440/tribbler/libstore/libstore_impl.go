package libstore

import (
	"errors"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	// TODO: implement this!
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
	return nil, errors.New("not implemented")
}

func (ls *libstore) Get(key string) (string, error) {
	// TODO: May need to use cache in the future.
	args := &storagerpc.GetArgs{key, false, ls.myHostPort}
	var reply *storagerpc.GetReply

	cli, err := GetStorageServer(key)
	if err {
		return nil, errors.New("Get error:", err)
	}

	err2 := cli.Call("StorageServer.Get", args, &reply)
	if err2 {
		return nil, errors.New("Get error:", err2)
	}

	if reply.Status != storagerpc.OK {
		return nil, errors.New("Status not OK. Got status:", reply.Status)
	}

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{key, value}
	var reply *storagerpc.PutReply

	err := ls.svr.Call("StorageServer.Put", args, &reply)
	if err {
		return errors.New("Put error:", err)
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK. Got status:", reply.Status)
	}

	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{key}
	var reply *storagerpc.DeleteReply

	err := ls.svr.Call("StorageServer.Delete", args, &reply)
	if err {
		return errors.New("Delete error:", err)
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK. Got status:", reply.Status)
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{key, false, ls.myHostPort}
	var reply *storagerpc.GetlistReply

	err := ls.svr.Call("StorageServer.GetList", args, &reply)
	if err {
		return nil, errors.New("GetList error:", err)
	}

	if reply.Status != storagerpc.OK {
		return nil, errors.New("Status not OK. Got status:", reply.Status)
	}

	return reply.Value, nil
}

func (ls *libstore) GetStorageServer(key string) (*rpc.Client, error) {
	// TODO: that as well.
	return ls.svr, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{key, removeItem}
	var reply *storagerpc.PutReply

	err := ls.svr.Call("StorageServer.RemoveFromList", args, &reply)
	if err {
		return errors.New("RemoveFromList error:", err)
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK. Got status:", reply.Status)
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{key, newItem}
	var reply *storagerpc.PutReply

	err := ls.svr.Call("StorageServer.AppendToList", args, &reply)
	if err {
		return errors.New("AppendToList error:", err)
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK. Got status:", reply.Status)
	}

	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
