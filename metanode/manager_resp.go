package metanode

import (
	"encoding/json"
	"net"

	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
)

const (
	masterResponsePath = "/metaNode/response" // Method: 'POST',
	// ContentType: 'application/json'
)

// ReplyToMaster reply operation result to master by sending http request.
func (m *metaManager) respondToMaster(data interface{}) (err error) {
	// Handle panic
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	// Process data and send reply though http specified remote address.
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	postToMaster("POST", masterResponsePath, jsonBytes)
	return
}

// ReplyToClient send reply data though tcp connection to client.
func (m *metaManager) respondToClient(conn net.Conn, p *Packet) (err error) {
	// Handle panic
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	// Process data and send reply though specified tcp connection.
	err = p.WriteToConn(conn)
	if err != nil {
		log.LogErrorf("response to client[%s], "+
			"request[%s], response packet[%s]",
			err.Error(), p.GetOpMsg(), p.GetResultMesg())
	}
	return
}

func (m *metaManager) responseAckOKToMaster(conn net.Conn, p *Packet) {
	go func() {
		p.PackOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
}
