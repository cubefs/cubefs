package master

import (
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
	"sort"
)

type UserService struct {
	user    *User
	cluster *Cluster
}

func (s *UserService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	s.registerObject(schema)
	s.registerQuery(schema)

	s.registerMutation(schema)

	return schema.MustBuild()
}

type UserStatistical struct {
	Data               uint64
	VolumeCount        int32
	DataPartitionCount int32
	MetaPartitionCount int32
}

type AuthorizedVols struct {
	Vol        string
	Authorized []string
}

func (s *UserService) registerObject(schema *schemabuilder.Schema) {

	object := schema.Object("UserInfo", proto.UserInfo{})

	object.FieldFunc("userStatistical", func(u *proto.UserInfo) (*UserStatistical, error) {
		us := &UserStatistical{
			VolumeCount: int32(len(u.Policy.OwnVols)),
		}
		for _, volName := range u.Policy.OwnVols {
			v, e := s.cluster.getVol(volName)
			if e != nil {
				return nil, e
			}
			us.MetaPartitionCount += int32(len(v.MetaPartitions))
			us.DataPartitionCount += int32(len(v.dataPartitions.partitions))
			us.Data += v.totalUsedSpace()
		}
		return us, nil
	})

	object = schema.Object("UserPolicy", proto.UserPolicy{})
	object.FieldFunc("authorizedVols", func(p *proto.UserPolicy) []AuthorizedVols {
		var list []AuthorizedVols
		for vol, a := range p.AuthorizedVols {
			list = append(list, AuthorizedVols{
				Vol:        vol,
				Authorized: a,
			})
		}
		return list
	})

}

func (s *UserService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()

	query.FieldFunc("getUserInfo", s.getUserInfo)
	query.FieldFunc("getUserAKInfo", s.getUserAKInfo)
	query.FieldFunc("validatePassword", s.validatePassword)
	query.FieldFunc("listUserInfo", s.listUserInfo)
	query.FieldFunc("topNUser", s.topNUser)
}

func (m *UserService) getUserAKInfo(ctx context.Context, args struct {
	AccessKey string
}) (*proto.UserInfo, error) {
	uid, perm, err := permissions(ctx, ADMIN|USER)
	userInfo, err := m.user.getKeyInfo(args.AccessKey)
	if err != nil {
		return nil, err
	}

	if perm != ADMIN {
		if uid != userInfo.UserID {
			return nil, fmt.Errorf("user info not found by you accesskey")
		}
	}

	return userInfo, nil
}

func (s *UserService) registerMutation(schema *schemabuilder.Schema) {
	mutation := schema.Mutation()

	mutation.FieldFunc("createUser", s.createUser)
	mutation.FieldFunc("updateUser", s.updateUser)
	mutation.FieldFunc("deleteUser", s.deleteUser)
	mutation.FieldFunc("updateUserPolicy", s.updateUserPolicy)
	mutation.FieldFunc("removeUserPolicy", s.removeUserPolicy)
	mutation.FieldFunc("transferUserVol", s.transferUserVol)

}

func (m *UserService) transferUserVol(ctx context.Context, args proto.UserTransferVolParam) (*proto.UserInfo, error) {
	uid, perm, err := permissions(ctx, ADMIN)
	if err != nil {
		return nil, err
	}

	vol, err := m.cluster.getVol(args.Volume)
	if err != nil {
		return nil, err
	}

	if perm == USER && vol.Owner != uid {
		return nil, fmt.Errorf("not have permission for vol:[%s]", args.Volume)
	}

	if !args.Force && vol.Owner != args.UserSrc {
		return nil, fmt.Errorf("force param need validate user name for vol:[%s]", args.Volume)
	}

	userInfo, err := m.user.transferVol(&args)
	if err != nil {
		return nil, err
	}
	owner := vol.Owner
	vol.Owner = userInfo.UserID
	if err = m.cluster.syncUpdateVol(vol); err != nil {
		vol.Owner = owner
		return nil, err
	}
	return userInfo, nil
}

func (s *UserService) updateUserPolicy(ctx context.Context, args proto.UserPermUpdateParam) (*proto.UserInfo, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	if _, err := s.cluster.getVol(args.Volume); err != nil {
		return nil, err
	}
	userInfo, err := s.user.updatePolicy(&args)
	if err != nil {
		return nil, err
	}

	return userInfo, nil
}

func (s *UserService) removeUserPolicy(ctx context.Context, args proto.UserPermRemoveParam) (*proto.UserInfo, error) {
	if _, err := s.cluster.getVol(args.Volume); err != nil {
		return nil, err
	}
	userInfo, err := s.user.removePolicy(&args)
	if err != nil {
		return nil, err
	}
	return userInfo, nil
}

func (s *UserService) createUser(ctx context.Context, args proto.UserCreateParam) (*proto.UserInfo, error) {
	uid, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return nil, err
	}

	if !ownerRegexp.MatchString(args.ID) {
		return nil, fmt.Errorf("user id:[%s] is invalid", args.ID)
	}
	if args.Type == proto.UserTypeRoot {
		return nil, fmt.Errorf("user type:[%s] can not to root", args.Type)
	}

	log.LogInfof("create user:[%s] by admin:[%s]", args.ID, uid)
	return s.user.createKey(&args)
}

func (s *UserService) updateUser(ctx context.Context, args proto.UserUpdateParam) (*proto.UserInfo, error) {
	uid, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return nil, err
	}

	old, err := s.user.getUserInfo(args.UserID)
	if err != nil {
		return nil, err
	}

	if old.UserType == proto.UserTypeRoot && args.Type != proto.UserTypeRoot  {
		return nil, fmt.Errorf("the root user is a build-in super user which can not change it to low-level user type")
	}
	if old.UserType != proto.UserTypeRoot && args.Type == proto.UserTypeRoot {
		return nil, fmt.Errorf("user type:[%s] can not to root", args.Type)
	}

	log.LogInfof("update user:[%s] by admin:[%s]", args.UserID, uid)
	return s.user.updateKey(&args)
}

func (s *UserService) deleteUser(ctx context.Context, args struct {
	UserID string
}) (*proto.GeneralResp, error) {
	uid, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return nil, err
	}

	//TODO : make sure can delete self? can delete other admin ??
	log.LogInfof("delete user:[%s] by admin:[%s]", args.UserID, uid)
	if err := s.user.deleteKey(args.UserID); err != nil {
		return nil, err
	}

	return proto.Success("del user ok"), nil

}

func (s *UserService) getUserInfo(ctx context.Context, args struct {
	UserID string
}) (*proto.UserInfo, error) {
	uid, perm, err := permissions(ctx, ADMIN|USER)
	if err != nil {
		return nil, err
	}

	if perm == USER {
		if uid != args.UserID {
			return nil, fmt.Errorf("you:[%s] not have permission visit this userID:[%s]", uid, args.UserID)
		}
	}

	return s.user.getUserInfo(args.UserID)
}

func (s *UserService) listUserInfo(ctx context.Context, args struct{}) ([]*proto.UserInfo, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	var list []*proto.UserInfo
	s.user.userStore.Range(func(_, ui interface{}) bool {
		list = append(list, ui.(*proto.UserInfo))
		return true
	})
	return list, nil
}

type UserUseSpace struct {
	Name  string
	Size  uint64
	Ratio float32
}

func (s *UserService) topNUser(ctx context.Context, args struct {
	N int32
}) ([]*UserUseSpace, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	list := make([]*UserUseSpace, 0)

	var err error
	s.user.userStore.Range(func(_, ui interface{}) bool {

		u := ui.(*proto.UserInfo)

		us := &UserUseSpace{
			Name:  u.UserID,
			Size:  0,
			Ratio: 0,
		}

		for _, volName := range u.Policy.OwnVols {
			v, e := s.cluster.getVol(volName)
			if e != nil {
				err = e
				return false
			}
			us.Size += v.totalUsedSpace()
		}

		list = append(list, us)
		return true
	})

	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i int, j int) bool {
		return list[i].Size > list[j].Size
	})

	if len(list) > 10 {
		list = list[:10]
	}

	var sum uint64
	for _, u := range list {
		sum += u.Size
	}

	for _, u := range list {
		if sum == 0 {
			u.Ratio = float32(1) / float32(len(list))
		} else {
			u.Ratio = float32(u.Size) / float32(sum)
		}

	}

	return list, nil
}

func (s *UserService) validatePassword(ctx context.Context, args struct {
	UserID   string
	Password string
}) (*proto.UserInfo, error) {
	ui, err := s.user.getUserInfo(args.UserID)
	if err != nil {
		return nil, err
	}

	ak, err := s.user.getAKUser(ui.AccessKey)
	if err != nil {
		return nil, err
	}

	if ak.Password != args.Password {
		log.LogWarnf("user:[%s] login pass word has err", args.UserID)
		return nil, fmt.Errorf("user or password has err")
	}
	return ui, nil
}

type permissionMode int

const ADMIN permissionMode = permissionMode(1)
const USER permissionMode = permissionMode(2)

func permissions(ctx context.Context, mode permissionMode) (userID string, perm permissionMode, err error) {
	userInfo := ctx.Value(proto.UserInfoKey).(*proto.UserInfo)

	userID = userInfo.UserID

	perm = USER
	if userInfo.UserType == proto.UserTypeRoot || userInfo.UserType == proto.UserTypeAdmin {
		perm = ADMIN
	}

	if ADMIN&mode == ADMIN {
		if perm == ADMIN {
			return
		}
	}

	if USER&mode == USER {
		if perm == USER {
			return
		}
	}

	err = fmt.Errorf("user:[%s] permissions has err:[%d] your:[%d]", userInfo.UserID, mode, perm)
	return
}
