package tribserver

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"time"
)

type tribServer struct {
	ls libstore.Libstore
}

type tribbleIDs []string

func (tribIdList tribbleIDs) Len() int {
	return len(tribIdList)
}

func (tribIdList tribbleIDs) Less(i, j int) bool {
	// For string i
	si := tribIdList[i]
	pi0 := strings.Index(si, ":")
	pi1 := strings.Index(si[pi0+1:], "_") + pi0 + 1
	pi2 := strings.Index(si[pi1+1:], "_") + pi1 + 1
	postTime_i, _ := strconv.ParseInt("0x"+si[pi1+1:pi2], 0, 64)

	// For string j
	sj := tribIdList[j]
	pj0 := strings.Index(sj, ":")
	pj1 := strings.Index(sj[pj0+1:], "_") + pj0 + 1
	pj2 := strings.Index(sj[pj1+1:], "_") + pj1 + 1
	postTime_j, _ := strconv.ParseInt("0x"+sj[pj1+1:pj2], 0, 64)

	return postTime_i > postTime_j // Because we want reverse order
}

func (tribIdList tribbleIDs) Swap(i, j int) {
	tribIdList[i], tribIdList[j] = tribIdList[j], tribIdList[i]
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, errors.New("Creating new libstore failed")
	}
	newTribServer := &tribServer{
		ls: libstore,
	}

	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	err = rpc.RegisterName("TribServer", tribrpc.Wrap(newTribServer))
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return newTribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)
	_, err := ts.ls.Get(useridkey)

	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	err2 := ts.ls.Put(useridkey, userid)
	if err2 != nil {
		reply.Status = tribrpc.Exists
		return err2
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	userid := args.UserID
	targetid := args.TargetUserID
	useridkey := util.FormatUserKey(userid)
	targetidkey := util.FormatUserKey(targetid)

	_, userExists := ts.ls.Get(useridkey)

	if userExists != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	_, targetExists := ts.ls.Get(targetidkey)

	if targetExists != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	userSubListKey := util.FormatSubListKey(userid)
	appendErr := ts.ls.AppendToList(userSubListKey, targetid)

	if appendErr != nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	userid := args.UserID
	targetid := args.TargetUserID
	useridkey := util.FormatUserKey(userid)
	targetidkey := util.FormatUserKey(targetid)

	_, userExists := ts.ls.Get(useridkey)

	if userExists != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	_, targetExists := ts.ls.Get(targetidkey)

	if targetExists != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	userSubListKey := util.FormatSubListKey(userid)
	removeErr := ts.ls.RemoveFromList(userSubListKey, targetid)

	if removeErr != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	_, userExists := ts.ls.Get(useridkey)

	if userExists != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userSubListKey := util.FormatSubListKey(userid)
	nodes, err := ts.ls.GetList(userSubListKey)

	if err != nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = nil
		return nil
	}

	reply.Status = tribrpc.OK
	reply.UserIDs = nodes
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	if args == nil {
		return errors.New("ts: Can't post nil")
	}
	if reply == nil {
		return errors.New("ts: Can't reply with nil in Post")
	}

	curTime := time.Now()
	_, err := ts.ls.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		reply.PostKey = ""
		return nil
	}

	newTribID := util.FormatPostKey(args.UserID, curTime.UnixNano())

	for ts.ls.AppendToList(util.FormatTribListKey(args.UserID), newTribID) != nil {
		newTribID = util.FormatPostKey(args.UserID, curTime.UnixNano())
	}

	trb := &tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   curTime,
		Contents: args.Contents,
	}

	marshaled, _ := json.Marshal(trb)
	if ts.ls.Put(newTribID, string(marshaled[:])) != nil {
		ts.ls.RemoveFromList(util.FormatTribListKey(args.UserID), newTribID)
		reply.Status = tribrpc.NoSuchUser
		reply.PostKey = ""
		return errors.New("Fatal, failed to add new post")
	}

	reply.Status = tribrpc.OK
	reply.PostKey = newTribID
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	if args == nil {
		return errors.New("ts: Can't delete nil")
	}
	if reply == nil {
		return errors.New("ts: Can't reply with nil in Delete")
	}

	_, err := ts.ls.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	err = ts.ls.RemoveFromList(util.FormatTribListKey(args.UserID), args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	if ts.ls.Delete(args.PostKey) != nil {
		reply.Status = tribrpc.NoSuchPost
		return errors.New("Fatal, failed to delete a key")
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	_, userExists := ts.ls.Get(useridkey)

	if userExists != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userTribListKey := util.FormatTribListKey(userid)
	tribbleIdList, err := ts.ls.GetList(userTribListKey)

	if err != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = nil
		return nil
	}

	tmp := tribbleIDs(tribbleIdList)
	sort.Sort(tmp)
	tribbleIdsChrono := []string(tmp)

	// Calculate the length of the tribbles we want to get
	length := 100
	if len(tribbleIdsChrono) < 100 {
		length = len(tribbleIdsChrono)
	}
	allTribbles := make([]tribrpc.Tribble, length)
	var tribTmp tribrpc.Tribble
	i := 0
	for i < length {
		trib, err := ts.ls.Get(tribbleIdsChrono[i])
		if err == nil {
			json.Unmarshal([]byte(trib), &tribTmp)
			allTribbles[i] = tribTmp
			i += 1 // If error occurs, we do NOT increment i
		}
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = allTribbles

	return nil

}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if args == nil {
		return errors.New("ts: Can't getSubscription nil")
	}
	if reply == nil {
		return errors.New("ts: Can't reply with nil in getSubscription")
	}

	_, err := ts.ls.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		reply.Tribbles = make([]tribrpc.Tribble, 0, 0)
		return nil
	}

	users, err := ts.ls.GetList(util.FormatSubListKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = make([]tribrpc.Tribble, 0, 0)
		return nil
	}

	postIDs := make([]string, 0)
	for _, userID := range users {
		lst, err := ts.ls.GetList(util.FormatTribListKey(userID))
		if err == nil {
			postIDs = append(postIDs, lst...)
		}
	}

	tmp := tribbleIDs(postIDs)

	sort.Sort(tmp)
	tribbleIdsChrono := []string(tmp)

	length := 100
	if len(tribbleIdsChrono) < 100 {
		length = len(tribbleIdsChrono)
	}

	tribbles := make([]tribrpc.Tribble, length)
	var tribTmp tribrpc.Tribble
	i := 0
	for i < length {
		trib, err := ts.ls.Get(tribbleIdsChrono[i])
		if err == nil {
			json.Unmarshal([]byte(trib), &tribTmp)
			tribbles[i] = tribTmp
			i += 1 // If error occurs, we do NOT increment i
		}
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles
	return nil
}
