package volume
//auto generral by sdk/graphql general.go

import "context"
import "github.com/cubefs/cubefs/sdk/graphql/client"


type VolumeClient struct {
	*client.MasterGClient
}

func NewVolumeClient(c *client.MasterGClient) *VolumeClient {
	return &VolumeClient{c}
}

//struct begin .....
type Token struct {
	TokenType	int8
	Value	string
	VolName	string
}

type UserPermission struct {
	Access	[]string
	Edit	bool
	UserID	string
}

type Vol struct {
	Capacity	uint64
	CreateTime	int64
	DpReplicaNum	int32
	FollowerRead	bool
	ID	uint64
	Name	string
	NeedToLowerReplica	bool
	OSSAccessKey	string
	OSSSecretKey	string
	Occupied	int64
	Owner	string
	RWMutex	RWMutex
	Status	uint8
	ToSimpleVolView	*SimpleVolView
	Tokens	[]Token
}

type GeneralResp struct {
	Code	int32
	Message	string
}

type RWMutex struct {
}

type SimpleVolView struct {
	Authenticate	bool
	Capacity	uint64
	CreateTime	string
	CrossZone	bool
	Description	string
	DpCnt	int
	DpReplicaNum	uint8
	EnableToken	bool
	FollowerRead	bool
	ID	uint64
	MpCnt	int
	MpReplicaNum	uint8
	Name	string
	NeedToLowerReplica	bool
	Owner	string
	RwDpCnt	int
	Status	uint8
	ZoneName	string
}

//function begin .....
func (c *VolumeClient) CreateVolume (ctx context.Context, authenticate bool, capacity uint64, crossZone bool, dataPartitionSize uint64, description string, dpReplicaNum uint64, enableToken bool, followerRead bool, mpCount uint64, name string, owner string, zoneName string) (*Vol, error){

		req := client.NewRequest(ctx, `mutation($authenticate: bool, $capacity: uint64, $crossZone: bool, $dataPartitionSize: uint64, $description: string, $dpReplicaNum: uint64, $enableToken: bool, $followerRead: bool, $mpCount: uint64, $name: string, $owner: string, $zoneName: string){
			createVolume(authenticate: $authenticate, capacity: $capacity, crossZone: $crossZone, dataPartitionSize: $dataPartitionSize, description: $description, dpReplicaNum: $dpReplicaNum, enableToken: $enableToken, followerRead: $followerRead, mpCount: $mpCount, name: $name, owner: $owner, zoneName: $zoneName){
				capacity
				createTime
				dpReplicaNum
				followerRead
				iD
				name
				needToLowerReplica
				oSSAccessKey
				oSSSecretKey
				occupied
				owner
				rWMutex{
				}
				status
				toSimpleVolView{
					authenticate
					capacity
					createTime
					crossZone
					description
					dpCnt
					dpReplicaNum
					enableToken
					followerRead
					iD
					mpCnt
					mpReplicaNum
					name
					needToLowerReplica
					owner
					rwDpCnt
					status
					zoneName
				}
				tokens{
					tokenType
					value
					volName
				}
			}

		}`)
	
		req.Var("authenticate", authenticate)
		req.Var("capacity", capacity)
		req.Var("crossZone", crossZone)
		req.Var("dataPartitionSize", dataPartitionSize)
		req.Var("description", description)
		req.Var("dpReplicaNum", dpReplicaNum)
		req.Var("enableToken", enableToken)
		req.Var("followerRead", followerRead)
		req.Var("mpCount", mpCount)
		req.Var("name", name)
		req.Var("owner", owner)
		req.Var("zoneName", zoneName)
	
		rep, err := c.Query(ctx, "/api/volume", req)
		if err != nil {
			return nil, err
		}
		result := Vol{}
	
		if err := rep.GetValueByType(&result, "createVolume"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *VolumeClient) DeleteVolume (ctx context.Context, authKey string, name string) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($authKey: string, $name: string){
			deleteVolume(authKey: $authKey, name: $name){
				code
				message
			}

		}`)
	
		req.Var("authKey", authKey)
		req.Var("name", name)
	
		rep, err := c.Query(ctx, "/api/volume", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "deleteVolume"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *VolumeClient) UpdateVolume (ctx context.Context, authKey string, authenticate *bool, capacity *uint64, description *string, enableToken *bool, followerRead *bool, name string, replicaNum *uint64, zoneName *string) (*Vol, error){

		req := client.NewRequest(ctx, `mutation($authKey: string, $authenticate: bool, $capacity: uint64, $description: string, $enableToken: bool, $followerRead: bool, $name: string, $replicaNum: uint64, $zoneName: string){
			updateVolume(authKey: $authKey, authenticate: $authenticate, capacity: $capacity, description: $description, enableToken: $enableToken, followerRead: $followerRead, name: $name, replicaNum: $replicaNum, zoneName: $zoneName){
				capacity
				createTime
				dpReplicaNum
				followerRead
				iD
				name
				needToLowerReplica
				oSSAccessKey
				oSSSecretKey
				occupied
				owner
				rWMutex{
				}
				status
				toSimpleVolView{
					authenticate
					capacity
					createTime
					crossZone
					description
					dpCnt
					dpReplicaNum
					enableToken
					followerRead
					iD
					mpCnt
					mpReplicaNum
					name
					needToLowerReplica
					owner
					rwDpCnt
					status
					zoneName
				}
				tokens{
					tokenType
					value
					volName
				}
			}

		}`)
	
		req.Var("authKey", authKey)
		req.Var("authenticate", authenticate)
		req.Var("capacity", capacity)
		req.Var("description", description)
		req.Var("enableToken", enableToken)
		req.Var("followerRead", followerRead)
		req.Var("name", name)
		req.Var("replicaNum", replicaNum)
		req.Var("zoneName", zoneName)
	
		rep, err := c.Query(ctx, "/api/volume", req)
		if err != nil {
			return nil, err
		}
		result := Vol{}
	
		if err := rep.GetValueByType(&result, "updateVolume"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *VolumeClient) GetVolume (ctx context.Context, name string) (*Vol, error){

		req := client.NewRequest(ctx, `query($name: string){
			getVolume(name: $name){
				capacity
				createTime
				dpReplicaNum
				followerRead
				iD
				name
				needToLowerReplica
				oSSAccessKey
				oSSSecretKey
				occupied
				owner
				rWMutex{
				}
				status
				toSimpleVolView{
					authenticate
					capacity
					createTime
					crossZone
					description
					dpCnt
					dpReplicaNum
					enableToken
					followerRead
					iD
					mpCnt
					mpReplicaNum
					name
					needToLowerReplica
					owner
					rwDpCnt
					status
					zoneName
				}
				tokens{
					tokenType
					value
					volName
				}
			}

		}`)
	
		req.Var("name", name)
	
		rep, err := c.Query(ctx, "/api/volume", req)
		if err != nil {
			return nil, err
		}
		result := Vol{}
	
		if err := rep.GetValueByType(&result, "getVolume"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *VolumeClient) ListVolume (ctx context.Context, keyword *string, userID *string) ([]Vol, error){

		req := client.NewRequest(ctx, `query($keyword: string, $userID: string){
			listVolume(keyword: $keyword, userID: $userID){
				capacity
				createTime
				dpReplicaNum
				followerRead
				iD
				name
				needToLowerReplica
				oSSAccessKey
				oSSSecretKey
				occupied
				owner
				rWMutex{
				}
				status
				toSimpleVolView{
					authenticate
					capacity
					createTime
					crossZone
					description
					dpCnt
					dpReplicaNum
					enableToken
					followerRead
					iD
					mpCnt
					mpReplicaNum
					name
					needToLowerReplica
					owner
					rwDpCnt
					status
					zoneName
				}
				tokens{
					tokenType
					value
					volName
				}
			}

		}`)
	
		req.Var("keyword", keyword)
		req.Var("userID", userID)
	
		rep, err := c.Query(ctx, "/api/volume", req)
		if err != nil {
			return nil, err
		}
		result := make([]Vol,0)
	
		if err := rep.GetValueByType(&result, "listVolume"); err != nil {
			return nil, err
		}
	
		return result, nil
	
}

func (c *VolumeClient) VolPermission (ctx context.Context, userID *string, volName string) ([]UserPermission, error){

		req := client.NewRequest(ctx, `query($userID: string, $volName: string){
			volPermission(userID: $userID, volName: $volName){
				access
				edit
				userID
			}

		}`)
	
		req.Var("userID", userID)
		req.Var("volName", volName)
	
		rep, err := c.Query(ctx, "/api/volume", req)
		if err != nil {
			return nil, err
		}
		result := make([]UserPermission,0)
	
		if err := rep.GetValueByType(&result, "volPermission"); err != nil {
			return nil, err
		}
	
		return result, nil
	
}

