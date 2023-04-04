package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cubefs/cubefs/console/cutil"
	. "github.com/cubefs/cubefs/objectnode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/graphql/client"
	"github.com/cubefs/cubefs/sdk/graphql/client/user"
	"github.com/cubefs/cubefs/util/log"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

type FileService struct {
	userClient *user.UserClient
	manager    *VolumeManager
	objectNode string
}

func NewFileService(objectNode string, masters []string, mc *client.MasterGClient) *FileService {
	return &FileService{
		manager:    NewVolumeManager(masters, true),
		userClient: &user.UserClient{mc},
		objectNode: objectNode,
	}
}

func (fs *FileService) empty(ctx context.Context, args struct {
	Empty bool
}) (bool, error) {

	vol := "test"

	userInfo, err := fs.userClient.GetUserInfoForLogin(ctx, "root")
	if err != nil {
		return false, err
	} else {

		policy := userInfo.Policy

		v := ""

		for _, ov := range policy.Own_vols {
			v = v + " " + ov + " " + vol + ","
			v = fmt.Sprintf(v+", {}", ov == vol)
		}

		return false, fmt.Errorf("%v , [%v] , [%v] , [%v]", userInfo.Policy.Own_vols, userInfo.User_id, fs.userVolPerm(ctx, "root", "test"), v)
	}

	return args.Empty, nil
}

type ListFileInfo struct {
	Infos       []*FSFileInfo
	NextMarker  string
	IsTruncated bool
	Prefixes    []string
}

func (fs *FileService) listFile(ctx context.Context, args struct {
	VolName string
	Request ListFilesV1Option
}) (*ListFileInfo, error) {
	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err := fs.userVolPerm(ctx, userInfo.User_id, args.VolName).read(); err != nil {
		return nil, err
	}

	volume, err := fs.manager.Volume(args.VolName)
	if err != nil {
		return nil, err
	}

	result, err := volume.ListFilesV1(&args.Request)

	if err != nil {
		return nil, err
	}

	return &ListFileInfo{
		Infos:       result.Files,
		NextMarker:  result.NextMarker,
		IsTruncated: result.Truncated,
		Prefixes:    result.CommonPrefixes,
	}, err
}

func (fs *FileService) createDir(ctx context.Context, args struct {
	VolName string
	Path    string
}) (*FSFileInfo, error) {

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err := fs.userVolPerm(ctx, userInfo.User_id, args.VolName).write(); err != nil {
		return nil, err
	}

	volume, err := fs.manager.Volume(args.VolName)
	if err != nil {
		return nil, err
	}

	return volume.PutObject(args.Path, nil, &PutFileOption{
		MIMEType: HeaderValueContentTypeDirectory,
		Tagging:  nil,
		Metadata: nil,
	})
}

func (fs *FileService) deleteDir(ctx context.Context, args struct {
	VolName string
	Path    string
}) (*proto.GeneralResp, error) {

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err := fs.userVolPerm(ctx, userInfo.User_id, args.VolName).write(); err != nil {
		return nil, err
	}

	volume, err := fs.manager.Volume(args.VolName)
	if err != nil {
		return nil, err
	}

	if err := _deleteDir(ctx, volume, args.Path, ""); err != nil {
		return nil, err
	}

	if err := volume.DeletePath(args.Path + "/"); err != nil {
		return nil, err
	}

	return proto.Success("success"), nil
}

func _deleteDir(ctx context.Context, volume *Volume, path string, marker string) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("time out")
	default:
	}

	for {
		result, err := volume.ListFilesV1(&ListFilesV1Option{
			Prefix:    path,
			Delimiter: "/",
			Marker:    marker,
			MaxKeys:   10000,
		})

		for _, prefixe := range result.CommonPrefixes {
			if err := _deleteDir(ctx, volume, prefixe, ""); err != nil {
				return err
			}
			if err := volume.DeletePath(prefixe + "/"); err != nil {
				return err
			}
		}

		for _, info := range result.Files {
			if err := volume.DeletePath(info.Path); err != nil {
				return err
			}
		}

		if err != nil {
			return err
		}

		if !result.Truncated {
			return nil
		}

		marker = result.NextMarker
	}

}

func (fs *FileService) deleteFile(ctx context.Context, args struct {
	VolName string
	Path    string
}) (*proto.GeneralResp, error) {

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err := fs.userVolPerm(ctx, userInfo.User_id, args.VolName).write(); err != nil {
		return nil, err
	}

	volume, err := fs.manager.Volume(args.VolName)
	if err != nil {
		return nil, err
	}

	if err := volume.DeletePath(args.Path); err != nil {
		return nil, err
	}

	return proto.Success("success"), nil
}

func (fs *FileService) fileMeta(ctx context.Context, args struct {
	VolName string
	Path    string
}) (info *FSFileInfo, err error) {

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err := fs.userVolPerm(ctx, userInfo.User_id, args.VolName).read(); err != nil {
		return nil, err
	}

	volume, err := fs.manager.Volume(args.VolName)
	if err != nil {
		return nil, err
	}
	return volume.ObjectMeta(args.Path, false)
}

func (fs *FileService) signURL(ctx context.Context, args struct {
	VolName       string
	Path          string
	ExpireMinutes int64
}) (*proto.GeneralResp, error) {
	if args.Path == "" || args.ExpireMinutes <= 0 || args.VolName == "" {
		return nil, fmt.Errorf("param has err")
	}

	userInfo, _, err := permissions(ctx, USER|ADMIN)
	if err != nil {
		return nil, err
	}

	if err := fs.userVolPerm(ctx, userInfo.User_id, args.VolName).read(); err != nil {
		return nil, err
	}

	sess := session.Must(session.NewSession())
	var ac = aws.NewConfig()
	ac.Endpoint = aws.String(fs.objectNode)
	ac.DisableSSL = aws.Bool(true)
	ac.Region = aws.String("default")
	ac.Credentials = credentials.NewStaticCredentials(userInfo.Access_key, userInfo.Secret_key, "")
	ac.S3ForcePathStyle = aws.Bool(true)
	s3Svc := s3.New(sess, ac)
	request, _ := s3Svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(args.VolName),
		Key:    aws.String(args.Path),
	})
	u, _, err := request.PresignRequest(time.Duration(args.ExpireMinutes) * time.Minute)
	return proto.Success(u), err
}

//?vol_name=abc&path=/aaa/bbb/ddd.txt
func (fs *FileService) DownFile(writer http.ResponseWriter, request *http.Request) error {
	if err := request.ParseForm(); err != nil {
		return err
	}

	token, err := cutil.Token(request)
	if err != nil {
		return err
	}

	info, err := cutil.TokenValidate(token)
	if err != nil {
		return err
	}
	ctx := context.WithValue(request.Context(), proto.UserKey, info.User_id)

	volNames := request.Form["vol_name"]
	var volName string
	if len(volNames) > 0 {
		volName = volNames[0]
	} else {
		return fmt.Errorf("not found path in get param ?vol_name=[your path]")
	}

	//author validate
	if err := fs.userVolPerm(ctx, info.User_id, volName).read(); err != nil {
		return fmt.Errorf("the user does not have permission to access this volume")
	}

	var path string
	paths := request.Form["path"]
	if len(paths) > 0 {
		path = paths[0]
	} else {
		return fmt.Errorf("not found path in get param ?path=[your path]")
	}

	volume, err := fs.manager.Volume(volName)
	if err != nil {
		return err
	}

	meta, err := volume.ObjectMeta(path, true)
	if err != nil {
		return err
	}

	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))

	if _, err := volume.ReadFile(path, writer, 0, uint64(meta.Size)); err != nil {
		return err
	}

	return nil
}

//?vol_name=abc&path=/aaa/bbb/ddd.txt {file}
func (fs *FileService) UpLoadFile(writer http.ResponseWriter, request *http.Request) error {
	if err := request.ParseForm(); err != nil {
		return err
	}

	token, err := cutil.Token(request)
	if err != nil {
		return err
	}

	info, err := cutil.TokenValidate(token)
	if err != nil {
		return err
	}
	ctx := context.WithValue(request.Context(), proto.UserKey, info.User_id)

	volNames := request.Form["vol_name"]
	var volName string
	if len(volNames) > 0 {
		volName = volNames[0]
	} else {
		return fmt.Errorf("not found path in get param ?vol_name=[your path]")
	}

	//author validate
	if err := fs.userVolPerm(ctx, info.User_id, volName).write(); err != nil {
		return fmt.Errorf("the user:[%s] does not have permission to access this volume:[%s]", info.User_id, volName)
	}

	var path string
	paths := request.Form["path"]
	if len(paths) > 0 {
		path = paths[0]
	} else {
		return fmt.Errorf("not found path in get param ?path=[your path]")
	}

	volume, err := fs.manager.Volume(volName)
	if err != nil {
		return err
	}

	file, header, err := request.FormFile("file")
	if err != nil {
		return fmt.Errorf("get file from request has err:[%s]", err.Error())
	}

	filepath := path + "/" + header.Filename

	object, err := volume.PutObject(filepath, file, &PutFileOption{
		MIMEType: header.Header.Get("Content-Type"),
		Tagging:  nil,
		Metadata: nil,
	})

	if err != nil {
		return fmt.Errorf("put to object has err:[%s]", err.Error())
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("charset", "utf-8")

	v, e := json.Marshal(object)
	if e != nil {
		return e
	}
	_, _ = writer.Write(v)

	return nil
}

type KeyValue struct {
	Key   string
	Value string
}

func (fs *FileService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()
	query := schema.Query()
	query.FieldFunc("fileMeta", fs.fileMeta)
	query.FieldFunc("listFile", fs.listFile)
	query.FieldFunc("_empty", fs.empty)

	mutation := schema.Mutation()
	mutation.FieldFunc("deleteFile", fs.deleteFile)
	mutation.FieldFunc("createDir", fs.createDir)
	mutation.FieldFunc("signURL", fs.signURL)
	mutation.FieldFunc("deleteDir", fs.deleteDir)

	object := schema.Object("FSFileInfo", FSFileInfo{})

	object.FieldFunc("FSFileInfo", func(f *FSFileInfo) []*KeyValue {
		list := make([]*KeyValue, 0, len(f.Metadata))
		for k, v := range f.Metadata {
			list = append(list, &KeyValue{Key: k, Value: v})
		}
		return list
	})

	return schema.MustBuild()
}

type volPerm int

var none = volPerm(0)
var read = volPerm(1)
var write = volPerm(2)

func (v volPerm) read() error {
	if v < read {
		return fmt.Errorf("do not have permission read")
	}
	return nil
}

func (v volPerm) write() error {
	if v < write {
		return fmt.Errorf("do not have permission write")
	}
	return nil
}

func (fs *FileService) userVolPerm(ctx context.Context, userID string, vol string) volPerm {

	userInfo, err := fs.userClient.GetUserInfoForLogin(ctx, userID)
	if err != nil {
		log.LogErrorf("found user by id:[%s] has err:[%s]", userID, err.Error())
		return none
	}

	policy := userInfo.Policy

	for _, ov := range policy.Own_vols {
		if ov == vol {
			return write
		}
	}

	pm := none

	for _, av := range policy.AuthorizedVols {
		if av.Vol == vol {
			for _, a := range av.Authorized {
				if strings.Contains(a, "ReadOnly") {
					if pm < read {
						pm = read
					}
				}
				if strings.Contains(a, "Write") {
					return write
				}
			}
		}
	}
	return pm
}
