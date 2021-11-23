package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
	"github.com/chubaofs/chubaofs/util/keystore"
)

// requst path
const (
	GetTicket      = "getticket"
	CreateKey      = "createkey"
	DeleteKey      = "deletekey"
	GetKey         = "getkey"
	AddCaps        = "addcaps"
	DeleteCaps     = "deletecaps"
	AddRaftNode    = "addraftnode"
	RemoveRaftNode = "removeraftnode"
	OSAddCaps      = "osaddcaps"
	OSDeleteCaps   = "osdeletecaps"
	OSGetCaps      = "osgetcaps"
	HTTP           = "http://"
	HTTPS          = "https://"
)

const (
	ID         = "id"
	Role       = "role"
	Caps       = "caps"
	AccessKey  = "access_key"
	AuthKey    = "auth_key"
	SessionKey = "session_key"
)

var action2PathMap = map[string]string{
	GetTicket:      proto.ClientGetTicket,
	CreateKey:      proto.AdminCreateKey,
	DeleteKey:      proto.AdminDeleteKey,
	GetKey:         proto.AdminGetKey,
	AddCaps:        proto.AdminAddCaps,
	DeleteCaps:     proto.AdminDeleteCaps,
	AddRaftNode:    proto.AdminAddRaftNode,
	RemoveRaftNode: proto.AdminRemoveRaftNode,
	OSAddCaps:      proto.OSAddCaps,
	OSDeleteCaps:   proto.OSDeleteCaps,
	OSGetCaps:      proto.OSGetCaps,
}

var (
	cflag    string
	flaginfo flagInfo
)

type ticketFlag struct {
	key     string
	host    string
	output  string
	request string
	service string
}

type apiFlag struct {
	ticket  string
	host    string
	service string
	request string
	data    string
	output  string
}

type flagInfo struct {
	ticket ticketFlag
	api    apiFlag
	https  httpsSetting
}

type keyRing struct {
	ID  string `json:"id"`
	Key []byte `json:"key"`
}

type ticketFile struct {
	ID        string `json:"id"`
	Key       string `json:"session_key"`
	ServiceID string `json:"service_id"`
	Ticket    string `json:"ticket"`
}

type httpsSetting struct {
	enable bool
	cert   []byte
}

func (m *ticketFile) dumpJSONFile(filename string) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		panic(err)
	}

	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = io.WriteString(file, string(data))
	if err != nil {
		panic(err)
	}
}

func sendReqX(target string, data interface{}, cert *[]byte) (res []byte, err error) {
	var (
		client *http.Client
	)
	target = HTTPS + target
	client, err = cryptoutil.CreateClientX(cert)
	if err != nil {
		return
	}
	res, err = proto.SendData(client, target, data)
	return
}

func sendReq(target string, data interface{}) (res []byte, err error) {
	target = HTTP + target
	client := &http.Client{}
	res, err = proto.SendData(client, target, data)
	return
}

func getTicketFromAuth(keyring *keyRing) (ticketfile ticketFile) {

	var (
		err     error
		ts      int64
		msgResp proto.AuthGetTicketResp
		body    []byte
	)

	// construct request body
	message := proto.AuthGetTicketReq{
		Type:      proto.MsgAuthTicketReq,
		ClientID:  keyring.ID,
		ServiceID: flaginfo.ticket.service,
	}

	if message.Verifier, ts, err = cryptoutil.GenVerifier(keyring.Key); err != nil {
		panic(err)
	}

	url := flaginfo.ticket.host + action2PathMap[flaginfo.ticket.request]

	if flaginfo.https.enable {
		body, err = sendReqX(url, message, &flaginfo.https.cert)
	} else {
		body, err = sendReq(url, message)
	}

	if err != nil {
		panic(err)
	}

	fmt.Printf("\n" + string(body) + "\n")

	if msgResp, err = proto.ParseAuthGetTicketResp(body, keyring.Key); err != nil {
		panic(err)
	}

	if err = proto.VerifyTicketRespComm(&msgResp, proto.MsgAuthTicketReq, keyring.ID, flaginfo.ticket.service, ts); err != nil {
		panic(err)
	}

	ticketfile.Ticket = msgResp.Ticket
	ticketfile.ServiceID = msgResp.ServiceID
	ticketfile.Key = cryptoutil.Base64Encode(msgResp.SessionKey.Key)
	ticketfile.ID = keyring.ID

	return
}

func getTicket() {
	cfg, err1 := config.LoadConfigFile(flaginfo.ticket.key)
	if err1 != nil {
		panic(err1)
	}
	key, err2 := cryptoutil.Base64Decode(cfg.GetString(AuthKey))
	if err2 != nil {
		panic(err2)
	}
	keyring := keyRing{
		ID:  cfg.GetString(ID),
		Key: key,
	}

	ticketfile := getTicketFromAuth(&keyring)
	ticketfile.dumpJSONFile(flaginfo.ticket.output)

	return
}

func accessAuthServer() {
	var (
		msg        proto.MsgType
		sessionKey []byte
		err        error
		message    interface{}
		ts         int64
		res        string
		body       []byte
	)

	switch flaginfo.api.request {
	case CreateKey:
		msg = proto.MsgAuthCreateKeyReq
	case DeleteKey:
		msg = proto.MsgAuthDeleteKeyReq
	case GetKey:
		msg = proto.MsgAuthGetKeyReq
	case AddCaps:
		msg = proto.MsgAuthAddCapsReq
	case DeleteCaps:
		msg = proto.MsgAuthDeleteCapsReq
	case AddRaftNode:
		msg = proto.MsgAuthAddRaftNodeReq
	case RemoveRaftNode:
		msg = proto.MsgAuthRemoveRaftNodeReq
	case OSAddCaps:
		msg = proto.MsgAuthOSAddCapsReq
	case OSDeleteCaps:
		msg = proto.MsgAuthOSDeleteCapsReq
	case OSGetCaps:
		msg = proto.MsgAuthOSGetCapsReq
	default:
		panic(fmt.Errorf("wrong requst [%s]", flaginfo.api.request))
	}

	ticketCFG, err := config.LoadConfigFile(flaginfo.api.ticket)
	if err != nil {
		panic(err)
	}

	apiReq := &proto.APIAccessReq{
		Type:      msg,
		ClientID:  ticketCFG.GetString(ID),
		ServiceID: proto.AuthServiceID,
	}

	if sessionKey, err = cryptoutil.Base64Decode(ticketCFG.GetString(SessionKey)); err != nil {
		panic(err)
	}

	if apiReq.Verifier, ts, err = cryptoutil.GenVerifier(sessionKey); err != nil {
		panic(err)
	}
	apiReq.Ticket = ticketCFG.GetString("ticket")

	dataCFG, err := config.LoadConfigFile(flaginfo.api.data)
	if err != nil {
		panic(err)
	}

	switch flaginfo.api.request {
	case CreateKey:
		message = proto.AuthAPIAccessReq{
			APIReq: *apiReq,
			KeyInfo: keystore.KeyInfo{
				ID:   dataCFG.GetString(ID),
				Role: dataCFG.GetString(Role),
				Caps: []byte(dataCFG.GetString(Caps)),
			},
		}
	case DeleteKey:
		fallthrough
	case GetKey:
		message = proto.AuthAPIAccessReq{
			APIReq: *apiReq,
			KeyInfo: keystore.KeyInfo{
				ID: dataCFG.GetString(ID),
			},
		}
	case AddCaps:
		fallthrough
	case DeleteCaps:
		message = proto.AuthAPIAccessReq{
			APIReq: *apiReq,
			KeyInfo: keystore.KeyInfo{
				ID:   dataCFG.GetString(ID),
				Caps: []byte(dataCFG.GetString(Caps)),
			},
		}
	case AddRaftNode:
		fallthrough
	case RemoveRaftNode:
		message = proto.AuthRaftNodeReq{
			APIReq: *apiReq,
			RaftNodeInfo: proto.AuthRaftNodeInfo{
				ID:   uint64(dataCFG.GetInt64(ID)),
				Addr: dataCFG.GetString("addr"),
			},
		}
	case OSAddCaps:
		fallthrough
	case OSDeleteCaps:
		message = proto.AuthOSAccessKeyReq{
			APIReq: *apiReq,
			AKCaps: keystore.AccessKeyCaps{
				AccessKey: dataCFG.GetString(AccessKey),
				Caps:      []byte(dataCFG.GetString(Caps)),
			},
		}
	case OSGetCaps:
		message = proto.AuthOSAccessKeyReq{
			APIReq: *apiReq,
			AKCaps: keystore.AccessKeyCaps{
				AccessKey: dataCFG.GetString(AccessKey),
			},
		}
	default:
		panic(fmt.Errorf("wrong action [%s]", flaginfo.api.request))
	}

	url := flaginfo.api.host + action2PathMap[flaginfo.api.request]

	if flaginfo.https.enable {
		body, err = sendReqX(url, message, &flaginfo.https.cert)
	} else {
		body, err = sendReq(url, message)
	}

	if err != nil {
		panic(err)
	}

	fmt.Printf("\nbody: " + string(body) + "\n")

	switch flaginfo.api.request {
	case CreateKey:
		fallthrough
	case DeleteKey:
		fallthrough
	case GetKey:
		fallthrough
	case AddCaps:
		fallthrough
	case DeleteCaps:
		var resp proto.AuthAPIAccessResp
		if resp, err = proto.ParseAuthAPIAccessResp(body, sessionKey); err != nil {
			panic(err)
		}

		if err = proto.VerifyAPIRespComm(&resp.APIResp, msg, ticketCFG.GetString(ID), proto.AuthServiceID, ts); err != nil {
			panic(err)
		}

		if flaginfo.api.request == CreateKey {
			if err = resp.KeyInfo.DumpJSONFile(flaginfo.api.output); err != nil {
				panic(err)
			}
		} else {
			if res, err = resp.KeyInfo.DumpJSONStr(); err != nil {
				panic(err)
			}
			fmt.Printf(res + "\n")
		}

	case AddRaftNode:
		fallthrough
	case RemoveRaftNode:
		var resp proto.AuthRaftNodeResp
		if resp, err = proto.ParseAuthRaftNodeResp(body, sessionKey); err != nil {
			panic(err)
		}

		if err = proto.VerifyAPIRespComm(&resp.APIResp, msg, ticketCFG.GetString(ID), proto.AuthServiceID, ts); err != nil {
			panic(err)
		}

		fmt.Printf(resp.Msg + "\n")
	case OSAddCaps:
		fallthrough
	case OSDeleteCaps:
		fallthrough
	case OSGetCaps:
		var resp proto.AuthOSAccessKeyResp
		if resp, err = proto.ParseAuthOSAKResp(body, sessionKey); err != nil {
			panic(err)
		}
		if err = proto.VerifyAPIRespComm(&resp.APIResp, msg, ticketCFG.GetString(ID), proto.AuthServiceID, ts); err != nil {
			panic(err)
		}
		if res, err = resp.AKCaps.DumpJSONStr(); err != nil {
			panic(err)
		}
		fmt.Printf(res + "\n")
	}

	return

}

func accessAPI() {
	switch flaginfo.api.service {
	case proto.AuthServiceID:
		accessAuthServer()
	default:
		panic(fmt.Errorf("server type error [%s]", flaginfo.api.service))
	}
}

func loadCertfile(path string) (caCert []byte) {
	var err error
	caCert, err = ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

func main() {
	ticketCmd := flag.NewFlagSet("ticket", flag.ExitOnError)
	apiCmd := flag.NewFlagSet("api", flag.ExitOnError)
	authkeyCmd := flag.NewFlagSet("authkey", flag.ExitOnError)
	signCmd := flag.NewFlagSet("sign", flag.ExitOnError)

	switch os.Args[1] {
	case "ticket":
		key := ticketCmd.String("keyfile", "keyring.json", "path to key file")
		host := ticketCmd.String("host", "localhost:8080", "api host")
		file := ticketCmd.String("output", "ticket.json", "output path to ticket file")
		https := ticketCmd.Bool("https", false, "enable https")
		certfile := ticketCmd.String("certfile", "server.crt", "path to cert file")
		ticketCmd.Parse(os.Args[2:])
		flaginfo.ticket.key = *key
		flaginfo.ticket.host = *host
		flaginfo.ticket.output = *file
		flaginfo.https.enable = *https
		if flaginfo.https.enable {
			flaginfo.https.cert = loadCertfile(*certfile)
		}
		if len(ticketCmd.Args()) >= 2 {
			flaginfo.ticket.request = ticketCmd.Args()[0]
			flaginfo.ticket.service = ticketCmd.Args()[1]
			if _, ok := action2PathMap[flaginfo.ticket.request]; !ok {
				panic(fmt.Errorf("illegal parameter %s", flaginfo.ticket.request))
			}
		}
		getTicket()
	case "api":
		ticket := apiCmd.String("ticketfile", "ticket.json", "path to ticket file")
		host := apiCmd.String("host", "localhost:8080", "api host")
		data := apiCmd.String("data", "data.json", "request data file")
		output := apiCmd.String("output", "keyring.json", "output path to keyring file")
		https := apiCmd.Bool("https", false, "enable https")
		certfile := apiCmd.String("certfile", "server.crt", "path to cert file")
		apiCmd.Parse(os.Args[2:])
		flaginfo.api.ticket = *ticket
		flaginfo.api.host = *host
		flaginfo.api.data = *data
		flaginfo.api.output = *output
		flaginfo.https.enable = *https
		if flaginfo.https.enable {
			flaginfo.https.cert = loadCertfile(*certfile)
		}
		if len(apiCmd.Args()) >= 2 {
			flaginfo.api.service = apiCmd.Args()[0]
			flaginfo.api.request = apiCmd.Args()[1]
			if _, ok := action2PathMap[flaginfo.api.request]; !ok {
				panic(fmt.Errorf("illegal parameter %s", flaginfo.api.request))
			}
		} else {
			panic(fmt.Errorf("requst parameter needed"))
		}
		accessAPI()
	case "authkey":
		len := authkeyCmd.Int("keylen", 32, "length of authkey")
		output := [2]*string{
			authkeyCmd.String("rootkey", "authroot.json", "output path to keyring file of auth root"),
			authkeyCmd.String("servicekey", "authservice.json", "output path to keyring file of service"),
		}
		authkeyCmd.Parse(os.Args[2:])
		for i := range output {
			random, err := generateRandomBytes(*len)
			if err != nil {
				panic(err)
			}
			keyInfo := keystore.KeyInfo{
				ID:      "AuthService",
				AuthKey: random,
				Ts:      time.Now().Unix(),
				Role:    "AuthService",
				Caps:    []byte(`{"*"}`),
			}
			keyInfo.DumpJSONFile(*output[i])
		}
	case "sign":
		userID := signCmd.String("user", "", "user ID")
		accessKey := signCmd.String("ak", "", "access key of user")
		secretKey := signCmd.String("sk", "", "secret key of user")
		path := signCmd.String("path", "", "API path")
		signCmd.Parse(os.Args[2:])

		if signCmd.NFlag() != 4 {
			flag.Usage()
			signCmd.PrintDefaults()
			os.Exit(1)
		}

		user := &proto.AuthUser{*userID, *accessKey, *secretKey}
		sign, err := user.GenerateSignature(*path)
		if err != nil {
			panic(fmt.Errorf("failed to generate signature: %v\n", err))
		}
		signature, err := json.Marshal(sign)
		if err != nil {
			panic(fmt.Errorf("failed to marshal signature: %v\n", err))
		}
		fmt.Println(string(signature))

	default:
		fmt.Println("expected 'ticket', 'api', 'authkey' or 'sign' subcommands")
		os.Exit(1)
	}
}
