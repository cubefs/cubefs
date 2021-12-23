package user

//auto generral by sdk/graphql general.go

import "context"
import "github.com/chubaofs/chubaofs/sdk/graphql/client"

type UserClient struct {
	*client.MasterGClient
}

func NewUserClient(c *client.MasterGClient) *UserClient {
	return &UserClient{c}
}

//struct begin .....
type AuthorizedVols struct {
	Authorized []string
	Vol        string
}

type GeneralResp struct {
	Code    int32
	Message string
}

type UserInfo struct {
	Access_key      string
	Create_time     string
	Description     string
	EMPTY           bool
	Policy          *UserPolicy
	Secret_key      string
	UserStatistical *UserStatistical
	User_id         string
	User_type       uint8
}

type UserPolicy struct {
	AuthorizedVols []AuthorizedVols
	Own_vols       []string
}

type UserStatistical struct {
	Data               uint64
	DataPartitionCount int32
	MetaPartitionCount int32
	VolumeCount        int32
}

type UserUseSpace struct {
	Name  string
	Ratio float32
	Size  uint64
}

//function begin .....
func (c *UserClient) DeleteUser(ctx context.Context, userID string) (*GeneralResp, error) {

	req := client.NewRequest(ctx, `mutation($userID: string){
			deleteUser(userID: $userID){
				code
				message
			}

		}`)

	req.Var("userID", userID)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := GeneralResp{}

	if err := rep.GetValueByType(&result, "deleteUser"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) UpdateUser(ctx context.Context, accessKey string, description string, secretKey string, Type uint8, userID string) (*UserInfo, error) {

	req := client.NewRequest(ctx, `mutation($accessKey: string, $description: string, $secretKey: string, $type: uint8, $userID: string){
			updateUser(accessKey: $accessKey, description: $description, secretKey: $secretKey, type: $type, userID: $userID){
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	req.Var("accessKey", accessKey)
	req.Var("description", description)
	req.Var("secretKey", secretKey)
	req.Var("type", Type)
	req.Var("userID", userID)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := UserInfo{}

	if err := rep.GetValueByType(&result, "updateUser"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) GetUserInfo(ctx context.Context, userID string) (*UserInfo, error) {

	req := client.NewRequest(ctx, `query($userID: string){
			getUserInfo(userID: $userID){
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	req.Var("userID", userID)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := UserInfo{}

	if err := rep.GetValueByType(&result, "getUserInfo"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) TopNUser(ctx context.Context, n int32) ([]UserUseSpace, error) {

	req := client.NewRequest(ctx, `query($n: int32){
			topNUser(n: $n){
				name
				ratio
				size
			}

		}`)

	req.Var("n", n)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := make([]UserUseSpace, 0)

	if err := rep.GetValueByType(&result, "topNUser"); err != nil {
		return nil, err
	}

	return result, nil

}

func (c *UserClient) ValidatePassword(ctx context.Context, password string, userID string) (*UserInfo, error) {

	req := client.NewRequest(ctx, `query($password: string, $userID: string){
			validatePassword(password: $password, userID: $userID){
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	req.Var("password", password)
	req.Var("userID", userID)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := UserInfo{}

	if err := rep.GetValueByType(&result, "validatePassword"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) CreateUser(ctx context.Context, accessKey string, description string, iD string, password string, secretKey string, Type uint8) (*UserInfo, error) {

	req := client.NewRequest(ctx, `mutation($accessKey: string, $description: string, $iD: string, $password: string, $secretKey: string, $type: uint8){
			createUser(accessKey: $accessKey, description: $description, iD: $iD, password: $password, secretKey: $secretKey, type: $type){
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	req.Var("accessKey", accessKey)
	req.Var("description", description)
	req.Var("iD", iD)
	req.Var("password", password)
	req.Var("secretKey", secretKey)
	req.Var("type", Type)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := UserInfo{}

	if err := rep.GetValueByType(&result, "createUser"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) RemoveUserPolicy(ctx context.Context, userID string, volume string) (*UserInfo, error) {

	req := client.NewRequest(ctx, `mutation($userID: string, $volume: string){
			removeUserPolicy(userID: $userID, volume: $volume){
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	req.Var("userID", userID)
	req.Var("volume", volume)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := UserInfo{}

	if err := rep.GetValueByType(&result, "removeUserPolicy"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) TransferUserVol(ctx context.Context, force bool, userDst string, userSrc string, volume string) (*UserInfo, error) {

	req := client.NewRequest(ctx, `mutation($force: bool, $userDst: string, $userSrc: string, $volume: string){
			transferUserVol(force: $force, userDst: $userDst, userSrc: $userSrc, volume: $volume){
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	req.Var("force", force)
	req.Var("userDst", userDst)
	req.Var("userSrc", userSrc)
	req.Var("volume", volume)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := UserInfo{}

	if err := rep.GetValueByType(&result, "transferUserVol"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) UpdateUserPolicy(ctx context.Context, policy []string, userID string, volume string) (*UserInfo, error) {

	req := client.NewRequest(ctx, `mutation($policy: []string, $userID: string, $volume: string){
			updateUserPolicy(policy: $policy, userID: $userID, volume: $volume){
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	req.Var("policy", policy)
	req.Var("userID", userID)
	req.Var("volume", volume)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := UserInfo{}

	if err := rep.GetValueByType(&result, "updateUserPolicy"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) GetUserAKInfo(ctx context.Context, accessKey string) (*UserInfo, error) {

	req := client.NewRequest(ctx, `query($accessKey: string){
			getUserAKInfo(accessKey: $accessKey){
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	req.Var("accessKey", accessKey)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := UserInfo{}

	if err := rep.GetValueByType(&result, "getUserAKInfo"); err != nil {
		return nil, err
	}

	return &result, nil

}

func (c *UserClient) ListUserInfo(ctx context.Context) ([]UserInfo, error) {

	req := client.NewRequest(ctx, `query(){
			listUserInfo{
				access_key
				create_time
				description
				eMPTY
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				secret_key
				userStatistical{
					data
					dataPartitionCount
					metaPartitionCount
					volumeCount
				}
				user_id
				user_type
			}

		}`)

	rep, err := c.Query(ctx, "/api/user", req)
	if err != nil {
		return nil, err
	}
	result := make([]UserInfo, 0)

	if err := rep.GetValueByType(&result, "listUserInfo"); err != nil {
		return nil, err
	}

	return result, nil

}
