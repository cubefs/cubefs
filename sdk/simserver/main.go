package main

//
// Usage: nohup ./simserver &
//
// This will simulate one master server and four meta nodes in localhost
// with "vol", "ports" and "inode ranges" hardcoded
// for testing purpose.
//

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaoio/cbfs/proto"
	. "github.com/chubaoio/cbfs/sdk/meta"
)

const (
	SimVolName    = "simserver"
	SimMasterPort = "8900"
	SimMetaAddr   = "localhost"

	SimLogFlags = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
)

type MetaNodeDesc struct {
	id    uint64
	start uint64
	end   uint64
	port  string
}

var globalMetaDesc = []MetaNodeDesc{
	{1, 1, 100, "8910"},
	{2, 101, 200, "8911"},
	{3, 210, 300, "8912"},
	{4, 301, 400, "8913"},
}

type MasterServer struct {
	ns map[string]*VolumeView
}

type MetaServer struct {
	sync.RWMutex
	start   uint64
	end     uint64
	port    string
	inodes  map[uint64]*Inode
	currIno uint64
}

type Inode struct {
	sync.RWMutex
	ino     uint64
	mode    uint32
	size    uint32
	extents []proto.ExtentKey
	dents   map[string]*Dentry
}

type Dentry struct {
	name string
	ino  uint64
	mode uint32
}

func main() {
	log.SetFlags(SimLogFlags)
	log.Println("Staring Sim Server ...")
	runtime.GOMAXPROCS(runtime.NumCPU())

	var wg sync.WaitGroup

	ms := NewMasterServer()
	ms.Start(&wg)

	for _, desc := range globalMetaDesc {
		mt := NewMetaServer(desc.start, desc.end, desc.port)
		mt.Start(&wg)
	}

	wg.Wait()
}

// Master Server

func NewMasterServer() *MasterServer {
	return &MasterServer{
		ns: make(map[string]*VolumeView),
	}
}

func (m *MasterServer) Start(wg *sync.WaitGroup) {
	nv := &VolumeView{
		VolName:        SimVolName,
		MetaPartitions: make([]*MetaPartition, 0),
	}

	for _, desc := range globalMetaDesc {
		mp := NewMetaPartition(desc.id, desc.start, desc.end, SimMetaAddr+":"+desc.port)
		nv.MetaPartitions = append(nv.MetaPartitions, mp)
	}

	m.ns[nv.VolName] = nv

	wg.Add(1)
	go func() {
		defer wg.Done()
		http.HandleFunc("/client/vol", m.handleClientNS)
		if err := http.ListenAndServe(":"+SimMasterPort, nil); err != nil {
			log.Println(err)
		} else {
			log.Println("Done!")
		}
	}()
}

func (m *MasterServer) handleClientNS(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	nv, ok := m.ns[name]
	if !ok {
		http.Error(w, "No such volume!", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(nv)
	if err != nil {
		http.Error(w, "JSON marshal failed!", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func NewMetaPartition(id, start, end uint64, member string) *MetaPartition {
	return &MetaPartition{
		PartitionID: id,
		Start:       start,
		End:         end,
		Members:     []string{member, member, member},
		LeaderAddr:  member,
	}
}

// Meta Server

func NewMetaServer(start, end uint64, port string) *MetaServer {
	return &MetaServer{
		start:   start,
		end:     end,
		port:    port,
		inodes:  make(map[uint64]*Inode),
		currIno: start - 1,
	}
}

func NewInode(ino uint64, mode uint32) *Inode {
	return &Inode{
		ino:     ino,
		mode:    mode,
		extents: make([]proto.ExtentKey, 0),
		dents:   make(map[string]*Dentry),
	}
}

func NewDentry(name string, ino uint64, mode uint32) *Dentry {
	return &Dentry{
		name: name,
		ino:  ino,
		mode: mode,
	}
}

func (m *MetaServer) Start(wg *sync.WaitGroup) {
	// Create root inode
	if m.start == proto.RootIno {
		m.allocIno()
		i := NewInode(proto.RootIno, proto.ModeDir)
		m.inodes[i.ino] = i
	}

	ln, err := net.Listen("tcp", ":"+m.port)
	if err != nil {
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go m.servConn(conn)
		}
	}()
}

func (m *MetaServer) servConn(conn net.Conn) {
	defer conn.Close()

	for {
		p := &proto.Packet{}
		if err := p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
			if err != io.EOF {
				log.Println("servConn ReadFromConn:", err)
			}
			return
		}

		if err := m.handlePacket(conn, p); err != nil {
			log.Println("servConn handlePacket:", err)
			return
		}
	}
}

func (m *MetaServer) handlePacket(conn net.Conn, p *proto.Packet) (err error) {
	switch p.Opcode {
	case proto.OpMetaCreateInode:
		err = m.handleCreateInode(conn, p)
	case proto.OpMetaCreateDentry:
		err = m.handleCreateDentry(conn, p)
	case proto.OpMetaDeleteInode:
		err = m.handleDeleteInode(conn, p)
	case proto.OpMetaDeleteDentry:
		err = m.handleDeleteDentry(conn, p)
	case proto.OpMetaLookup:
		err = m.handleLookup(conn, p)
	case proto.OpMetaReadDir:
		err = m.handleReadDir(conn, p)
	case proto.OpMetaInodeGet:
		err = m.handleInodeGet(conn, p)
	case proto.OpMetaExtentsAdd:
		err = m.handleAppendExtentKey(conn, p)
	case proto.OpMetaExtentsList:
		err = m.handleGetExtents(conn, p)
	default:
		err = errors.New("unknown Opcode: ")
	}
	return
}

func (m *MetaServer) handleCreateInode(conn net.Conn, p *proto.Packet) error {
	var (
		data  []byte
		inode *Inode
	)

	resp := &proto.CreateInodeResponse{}
	req := &proto.CreateInodeRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	ino, ok := m.allocIno()
	if !ok {
		p.ResultCode = proto.OpInodeFullErr
		goto out
	}

	inode = NewInode(ino, req.Mode)
	resp.Info = NewInodeInfo(ino, req.Mode, 0)

	data, err = json.Marshal(resp)
	if err != nil {
		p.ResultCode = proto.OpErr
		goto out
	}

	m.addInode(inode)
	p.ResultCode = proto.OpOk

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleCreateDentry(conn net.Conn, p *proto.Packet) error {
	req := &proto.CreateDentryRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	dentry := NewDentry(req.Name, req.Inode, req.Mode)

	parent := m.getInode(req.ParentID)
	if parent == nil {
		p.ResultCode = proto.OpNotExistErr
		log.Printf("Parent(%v) Not Exist", req.ParentID)
		goto out
	}

	if found := parent.addDentry(dentry); found != nil {
		p.ResultCode = proto.OpExistErr
		log.Printf("Parent(%v) Exist", req.ParentID)
		goto out
	}

	p.ResultCode = proto.OpOk
out:
	p.Data = nil
	p.Size = 0
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleDeleteInode(conn net.Conn, p *proto.Packet) error {
	var data []byte

	resp := &proto.DeleteInodeResponse{}
	req := &proto.DeleteInodeRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	ino := req.Inode
	inode := m.deleteInode(ino)
	if inode == nil {
		p.ResultCode = proto.OpNotExistErr
		goto out
	}

	resp.Extents = inode.extents
	if data, err = json.Marshal(resp); err != nil {
		log.Println(err)
		p.ResultCode = proto.OpErr
	} else {
		p.ResultCode = proto.OpOk
	}

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleDeleteDentry(conn net.Conn, p *proto.Packet) error {
	var (
		data  []byte
		child *Dentry
	)

	resp := &proto.DeleteDentryResponse{}
	req := &proto.DeleteDentryRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	parent := m.getInode(req.ParentID)
	if parent == nil {
		p.ResultCode = proto.OpErr
		goto out
	}

	child = parent.deleteDentry(req.Name)
	if child == nil {
		p.ResultCode = proto.OpNotExistErr
		goto out
	}

	resp.Inode = child.ino
	data, err = json.Marshal(resp)
	p.ResultCode = proto.OpOk

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleLookup(conn net.Conn, p *proto.Packet) error {
	var (
		data   []byte
		dentry *Dentry
	)

	resp := &proto.LookupResponse{}
	req := &proto.LookupRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	parent := m.getInode(req.ParentID)
	if parent == nil {
		p.ResultCode = proto.OpErr
		goto out
	}

	dentry = parent.getDentry(req.Name)
	if dentry == nil {
		p.ResultCode = proto.OpNotExistErr
		goto out
	}

	resp.Inode = dentry.ino
	resp.Mode = dentry.mode
	data, _ = json.Marshal(resp)
	p.ResultCode = proto.OpOk

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleReadDir(conn net.Conn, p *proto.Packet) error {
	var (
		data []byte
	)

	resp := &proto.ReadDirResponse{}
	req := &proto.ReadDirRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		return err
	}

	parent := m.getInode(req.ParentID)
	if parent == nil {
		p.ResultCode = proto.OpNotExistErr
		goto out
	}

	resp.Children = parent.listDentry()
	data, _ = json.Marshal(resp)
	p.ResultCode = proto.OpOk

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleInodeGet(conn net.Conn, p *proto.Packet) error {
	var data []byte

	resp := &proto.InodeGetResponse{}
	req := &proto.InodeGetRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		log.Println(err)
		return err
	}

	inode := m.getInode(req.Inode)
	if inode == nil {
		p.ResultCode = proto.OpNotExistErr
		goto out
	}

	resp.Info = NewInodeInfo(inode.ino, inode.mode, inode.size)
	if data, err = json.Marshal(resp); err != nil {
		log.Println(err)
		p.ResultCode = proto.OpErr
	} else {
		p.ResultCode = proto.OpOk
	}

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleAppendExtentKey(conn net.Conn, p *proto.Packet) error {
	var data []byte
	var cnt int

	req := &proto.AppendExtentKeyRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("handleAppendExtentKey: ", *req)
	inode := m.getInode(req.Inode)
	if inode == nil {
		p.ResultCode = proto.OpNotExistErr
		goto out
	}

	inode.Lock()
	cnt = len(inode.extents)
	if cnt <= 0 {
		inode.extents = append(inode.extents, req.Extent)
		inode.size = req.Extent.Size
	} else {
		ek := inode.extents[cnt-1]
		if ek.PartitionId != req.Extent.PartitionId || ek.ExtentId != req.Extent.ExtentId {
			inode.extents = append(inode.extents, req.Extent)
			inode.size += req.Extent.Size
		} else {
			if req.Extent.Size > ek.Size {
				inode.extents = append(inode.extents[:cnt-1], req.Extent)
				inode.size += (req.Extent.Size - ek.Size)
			}
		}
	}
	log.Println("handleAppendExtentKey: size = ", inode.size)
	inode.Unlock()
	p.ResultCode = proto.OpOk

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func (m *MetaServer) handleGetExtents(conn net.Conn, p *proto.Packet) error {
	var data []byte

	resp := &proto.GetExtentsResponse{}
	req := &proto.GetExtentsRequest{}
	err := json.Unmarshal(p.Data, req)
	if err != nil {
		log.Println(err)
		return err
	}

	inode := m.getInode(req.Inode)
	if inode == nil {
		p.ResultCode = proto.OpNotExistErr
		goto out
	}

	inode.RLock()
	resp.Extents = inode.extents
	if data, err = json.Marshal(resp); err != nil {
		log.Println(err)
		p.ResultCode = proto.OpErr
	} else {
		p.ResultCode = proto.OpOk
	}
	inode.RUnlock()

out:
	p.Data = data
	p.Size = uint32(len(data))
	err = p.WriteToConn(conn)
	return err
}

func NewInodeInfo(ino uint64, mode, size uint32) *proto.InodeInfo {
	return &proto.InodeInfo{
		Inode:      ino,
		Mode:       mode,
		Size:       uint64(size),
		ModifyTime: time.Now(),
		AccessTime: time.Now(),
		CreateTime: time.Now(),
	}
}

func NewDentryInfo(name string, ino uint64, mode uint32) *proto.Dentry {
	return &proto.Dentry{
		Name:  name,
		Inode: ino,
		Type:  mode,
	}
}

func (m *MetaServer) addInode(i *Inode) *Inode {
	m.Lock()
	defer m.Unlock()
	inode, ok := m.inodes[i.ino]
	if ok {
		return inode
	}
	m.inodes[i.ino] = i
	return nil
}

func (m *MetaServer) deleteInode(ino uint64) *Inode {
	m.Lock()
	defer m.Unlock()
	inode, ok := m.inodes[ino]
	if ok {
		delete(m.inodes, ino)
		return inode
	}
	return nil
}

func (m *MetaServer) getInode(ino uint64) *Inode {
	m.RLock()
	defer m.RUnlock()
	i, ok := m.inodes[ino]
	if !ok {
		return nil
	}
	return i
}

func (m *MetaServer) allocIno() (uint64, bool) {
	ino := atomic.AddUint64(&m.currIno, 1)
	if ino < m.start || ino > m.end {
		return 0, false
	}
	return ino, true
}

func (i *Inode) addDentry(d *Dentry) *Dentry {
	i.Lock()
	defer i.Unlock()
	log.Printf("Adding Dentry %v", *d)
	dentry, ok := i.dents[d.name]
	if ok {
		return dentry
	}
	i.dents[d.name] = d
	return nil
}

func (i *Inode) deleteDentry(name string) *Dentry {
	i.Lock()
	defer i.Unlock()
	dentry, ok := i.dents[name]
	if ok {
		delete(i.dents, name)
		return dentry
	}
	return nil
}

func (i *Inode) getDentry(name string) *Dentry {
	i.Lock()
	defer i.Unlock()
	dentry, ok := i.dents[name]
	if ok {
		return dentry
	}
	return nil
}

func (i *Inode) listDentry() []proto.Dentry {
	dentries := make([]proto.Dentry, 0)
	i.RLock()
	i.RUnlock()
	for _, d := range i.dents {
		dentry := NewDentryInfo(d.name, d.ino, d.mode)
		dentries = append(dentries, *dentry)
	}
	return dentries
}
