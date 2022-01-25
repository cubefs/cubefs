package user

import "context"
import "github.com/cubefs/cubefs/sdk/graphql/client"

func (c *UserClient) GetUserInfoForLogin(ctx context.Context, userID string) (*UserInfo, error) {

	req := client.NewRequest(ctx, `query($userID: string){
			getUserInfo(userID: $userID){
				access_key
				secret_key
				policy{
					authorizedVols{
						authorized
						vol
					}
					own_vols
				}
				user_type
				user_id
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
