package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/tiglabs/raft"
)

type HTTPReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func (s *testServer) startHttpService(host string, listen string) {
	http.HandleFunc("/data/submit", s.batchPutData)
	http.HandleFunc("/data/bigSubmit", s.putBigData)
	http.HandleFunc("/data/localbigSubmit", s.localBigData)
	http.HandleFunc("/data/get", s.getData)
	http.HandleFunc("/raft/addMember", s.addRaftMember)
	http.HandleFunc("/raft/delMember", s.delRaftMember)
	http.HandleFunc("/raft/status", s.getRaftStatus)
	http.HandleFunc("/raft/leader", s.getLeader)
	http.HandleFunc("/raft/tryLeader", s.tryLeader)
	http.HandleFunc("/raft/create", s.createRaft)

	addr := fmt.Sprintf("%v:%v", host, listen)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		panic(err)
	}
}

func (s *testServer) batchPutData(w http.ResponseWriter, r *http.Request) {
	var (
		step   int
		raftId int
		paral  int
		err    error
	)
	if err = r.ParseForm(); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse form err: %v", err)})
		return
	}
	if stepKey := r.FormValue("step"); stepKey != "" {
		step, err = strconv.Atoi(stepKey)
	}
	if step <= 0 || err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse step[%v] err: %v", step, err)})
		return
	}
	if id := r.FormValue("id"); id != "" {
		raftId, err = strconv.Atoi(id)
	}
	if raftId <= 0 || err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse raft id[%v] err: %v", raftId, err)})
		return
	}
	if paralKey := r.FormValue("paral"); paralKey != "" {
		paral, err = strconv.Atoi(paralKey)
	}
	startIndex := len(s.sm[uint64(raftId)].data)
	if paral > 1 {
		//if err = s.putDataParallel(uint64(raftId), startIndex, step, paral, bufW); err != nil {
		//	sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("raft[%v] parallel put data err: %v", raftId, err)})
		//	return
		//}
	} else {
		if err = s.putDataOnly(uint64(raftId), startIndex, step); err != nil {
			sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("raft[%v] put data err: %v", raftId, err),
				Data: s.raft.Status(uint64(raftId)).Leader})
			return
		}
	}
	sendReply(w, r, &HTTPReply{Code: 0, Msg: fmt.Sprintf("raft[%v] put data success", raftId)})
	return
}

var begin int32

func (s *testServer) localBigData(w http.ResponseWriter, r *http.Request) {
	var (
		size            int
		raftId          int
		exeMin          int
		err             error
		rst             string
		goroutingNumber int
	)

	if !atomic.CompareAndSwapInt32(&begin, 0, 1) {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("last local big data test is running.")})
		return
	}
	defer func() {
		atomic.CompareAndSwapInt32(&begin, 1, 0)
	}()

	if err = r.ParseForm(); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse form err: %v", err)})
		return
	}
	if sizeKey := r.FormValue("size"); sizeKey != "" {
		size, err = strconv.Atoi(sizeKey)
	}
	if size <= 0 || err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse size[%v] err: %v", size, err)})
		return
	}
	if id := r.FormValue("id"); id != "" {
		raftId, err = strconv.Atoi(id)
	}
	if raftId <= 0 || err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse raft id[%v] err: %v", raftId, err)})
		return
	}

	if !s.raft.IsLeader(uint64(raftId)) {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("the node is not raft id[%v] leader err: %v", raftId, err)})
		return
	}

	if min := r.FormValue("min"); min != "" {
		exeMin, err = strconv.Atoi(min)
	}
	if exeMin <= 0 || err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse min[%v] err: %v", exeMin, err)})
		return
	}

	if goroutingNum := r.FormValue("goroutings"); goroutingNum != "" {
		goroutingNumber, err = strconv.Atoi(goroutingNum)
	}
	if goroutingNumber <= 0 || err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse goroutings[%v] err: %v", goroutingNumber, err)})
		return
	}

	fmt.Printf("local bigdata submit: raftid-%d, datasize-%d, execute min-%d", raftId, size, exeMin)

	if err, rst = s.localPutBigData(uint64(raftId), size, exeMin, goroutingNumber); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("raft[%v] put data err: %v", raftId, err),
			Data: s.raft.Status(uint64(raftId)).Leader})
		return
	}

	sendReply(w, r, &HTTPReply{Code: 0, Msg: fmt.Sprintf("raft[%v] put data success:%s", raftId, rst)})

	return
}

func (s *testServer) putBigData(w http.ResponseWriter, r *http.Request) {
	var (
		size   int
		raftId int
		err    error
	)
	if err = r.ParseForm(); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse form err: %v", err)})
		return
	}
	if sizeKey := r.FormValue("size"); sizeKey != "" {
		size, err = strconv.Atoi(sizeKey)
	}
	if size <= 0 || err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse size[%v] err: %v", size, err)})
		return
	}
	if id := r.FormValue("id"); id != "" {
		raftId, err = strconv.Atoi(id)
	}
	if raftId <= 0 || err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse raft id[%v] err: %v", raftId, err)})
		return
	}
	if err = s.putOneBigData(uint64(raftId), size); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("raft[%v] put data err: %v", raftId, err),
			Data: s.raft.Status(uint64(raftId)).Leader})
		return
	}
	sendReply(w, r, &HTTPReply{Code: 0, Msg: fmt.Sprintf("raft[%v] put data success", raftId)})
	return
}

func (s *testServer) getData(w http.ResponseWriter, r *http.Request) {
	var (
		raftId uint64
		err    error
	)
	if raftId, err = parseRaftId(r); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse raft id[%v] err: %v", raftId, err)})
		return
	}
	reply := &HTTPReply{
		Code: 0,
		Msg:  fmt.Sprintf("raft[%v] get data success", raftId),
		Data: s.sm[raftId].data,
	}
	sendReply(w, r, reply)
	return
}

func (s *testServer) getRaftStatus(w http.ResponseWriter, r *http.Request) {
	var (
		raftId uint64
		err    error
	)
	if raftId, err = parseRaftId(r); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse raft id[%v] err: %v", raftId, err)})
		return
	}
	status := s.raft.Status(raftId)
	sendReply(w, r, &HTTPReply{Code: 0, Msg: fmt.Sprintf("get raft[%v] status success", raftId), Data: status})
	return
}

func (s *testServer) getLeader(w http.ResponseWriter, r *http.Request) {
	var (
		raftId uint64
		err    error
	)
	if raftId, err = parseRaftId(r); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse raft id[%v] err: %v", raftId, err)})
		return
	}
	status := s.raft.Status(raftId)
	sendReply(w, r, &HTTPReply{Code: 0, Msg: fmt.Sprintf("get raft[%v] leader success", raftId), Data: status.Leader})
	return
}

func (s *testServer) tryLeader(w http.ResponseWriter, r *http.Request) {
	var (
		raftId uint64
		err    error
	)
	if raftId, err = parseRaftId(r); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse raft id[%v] err: %v", raftId, err)})
		return
	}
	future := s.raft.TryToLeader(raftId)
	if _, err = future.Response(); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("try raft id[%v] to leader err: %v", raftId, err)})
		return
	}
	sendReply(w, r, &HTTPReply{Code: 0, Msg: fmt.Sprintf("try raft id[%v] to leader success", raftId)})
	return
}

func (s *testServer) createRaft(w http.ResponseWriter, r *http.Request) {
	var (
		num int
		err error
	)
	if err = r.ParseForm(); err != nil {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("parse form err: %v", err)})
		return
	}
	if numKey := r.FormValue("num"); numKey != "" {
		num, err = strconv.Atoi(numKey)
	}
	orgRaftNum := len(s.sm)
	if orgRaftNum >= num {
		sendReply(w, r, &HTTPReply{Code: 1, Msg: fmt.Sprintf("original raft groups num: %v", orgRaftNum)})
		return
	}
	for i := orgRaftNum + 1; i <= num; i++ {
		sm := newMemoryStatemachine(uint64(i), s.raft)
		st := getMemoryStorage(s.raft, s.nodeID, uint64(i))
		raftConfig := &raft.RaftConfig{
			ID:           uint64(i),
			Peers:        peers,
			Term:         0,
			Leader:       0,
			Storage:      st,
			StateMachine: sm,
		}
		err = s.raft.CreateRaft(raftConfig)
		if err != nil {
			panic(err)
		}
		s.sm[uint64(i)] = sm
		s.store[uint64(i)] = st
		subTimeMap[uint64(i)] = &subTime{minSubTime: math.MaxInt64, maxSubTime: 0, totalSubTime: 0, subCount: 0}
	}
	sendReply(w, r, &HTTPReply{Code: 0, Msg: fmt.Sprintf("create raft num[%v] to leader success", num)})
	return
}

func (s *testServer) addRaftMember(w http.ResponseWriter, r *http.Request) {
	// todo
}

func (s *testServer) delRaftMember(w http.ResponseWriter, r *http.Request) {
	// todo
}

func parseRaftId(r *http.Request) (id uint64, err error) {
	var raftId int
	if err = r.ParseForm(); err != nil {
		return
	}
	if idStr := r.FormValue("id"); idStr != "" {
		raftId, err = strconv.Atoi(idStr)
	}
	if raftId <= 0 || err != nil {
		return
	}
	return uint64(raftId), nil
}

func sendReply(w http.ResponseWriter, r *http.Request, reply *HTTPReply) {
	//fmt.Println(fmt.Sprintf("send url[%v], reply[%v]", r.URL, reply))
	bytes, err := json.Marshal(reply)
	if err != nil {
		fmt.Println("marshal reply err: ", err)
		return
	}
	_, err = w.Write(bytes)
	if err != nil {
		fmt.Println("send reply err: ", err)
		return
	}
	return
}
