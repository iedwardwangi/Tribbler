package tribserver

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"math"
	"sort"
	"time"
)

type tribServer struct {
	// TODO: implement this!
}

type tribbleIDs []string

func (tribIdList tribbleIDs) Len() int {
	return len(tribIdList)
}

func (tribIdList tribbleIDs) Less(i, j int) bool {
	// For string i
	si := tribIdList[i]
	pi0 := strings.Index(si, ":")
	si = si[pi0+1:]
	pi1 := strings.Index(si, "_") + pi0 + 1
	pi2 := strings.Index(si[pi1+1:], "_") + pi1 + 1
	postTime_i, _ := strconv.Atoi(si[pi1+1 : pi2])
	tieBreaker_i, _ := strconv.Atoi(si[pi2+1:])

	// For string j
	sj := tribIdList[j]
	pj0 := strings.Index(sj, ":")
	sj = si[pj0+1:]
	pj1 := strings.Index(sj, "_") + pj0 + 1
	pj2 := strings.Index(sj[pj1+1:], "_") + pj1 + 1
	postTime_j, _ := strconv.Atoi(sj[pj1+1 : pj2])
	tieBreaker_j, _ := strconv.Atoi(sj[pj2+1:])

	if postTime_i == postTime_j {
		return tieBreaker_i > tieBreaker_j
	} else {
		return postTime_i > postTime_j // Because we want reverse order
	}
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
	return nil, errors.New("not implemented")
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)
	_, err := ts.ls.Get(useridkey)

	if err == nil {
		reply.Status = tribrpc.Exists
		return errors.New("User already existed.")
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

	if userExists == nil {
		reply.Status = tribrpc.NoSuchUser
		return errors.New("No such user.")
	}

	_, targetExists := ts.ls.Get(targetidkey)

	if targetExists == nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return errors.New("No such target user.")
	}

	userSubListKey := util.FormatSubListKey(userid)
	appendErr := ts.ls.AppendToList(userSubListKey, targetid)

	if appendErr != nil {
		return appendErr
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

	if userExists == nil {
		reply.Status = tribrpc.NoSuchUser
		return errors.New("No such user.")
	}

	_, targetExists := ts.ls.Get(targetidkey)

	if targetExists == nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return errors.New("No such target user.")
	}

	userSubListKey := util.FormatSubListKey(userid)
	removeErr := ts.ls.RemoveFromList(userSubListKey, targetid)

	if removeErr != nil {
		return removeErr
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	_, userExists := ts.ls.Get(useridkey)

	if userExists == nil {
		reply.Status = tribrpc.NoSuchUser
		return errors.New("No such user.")
	}

	userSubListKey := util.FormatSubListKey(userid)
	nodes, err := ts.ls.GetList(userSubListKey)

	if err != nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = nil
		return err
	}

	reply.Status = tribrpc.OK
	reply.UserIDs = nodes
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	_, userExists := ts.ls.Get(useridkey)

	if userExists == nil {
		reply.Status = tribrpc.NoSuchUser
		return errors.New("No such user.")
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
	length := math.Min(len(tribbleIdsChrono), 100)
	allTribbles := make([]tribrpc.Tribble, length)
	var tribTmp tribrpc.Tribble
	i := 0
	for i < length {
		trib, err := ts.ls.Get(tribbleIdsChrono[i])
		if err == nil {
			json.Unmarshal([]byte(trib), &tribTmp)
			allTribbles[i] = trib
			i += 1 // If error occurs, we do NOT increment i
		}
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = allTribbles

	return nil

}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
