package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/graphql/client"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type MonitorService struct {
	cfg *cutil.ConsoleConfig
	cli *client.MasterGClient
}

func NewMonitorService(cfg *cutil.ConsoleConfig, cli *client.MasterGClient) *MonitorService {

	return &MonitorService{
		cfg: cfg,
		cli: cli,
	}
}

func (ms *MonitorService) empty(ctx context.Context, args struct {
	Empty bool
}) bool {
	return args.Empty
}

type MachineVersion struct {
	IP          string
	VersionValue *proto.VersionValue
	Message     string
}

func ErrMachineVersion(ip string, model string, err error) *MachineVersion {
	return &MachineVersion{
		IP: ip,
		VersionValue: &proto.VersionValue{
			Model: model,
		},
		Message: err.Error(),
	}
}

func (ms *MonitorService) VersionCheck(ctx context.Context, args struct{}) ([]*MachineVersion, error) {
	var result []*MachineVersion

	vi := proto.MakeVersion("console")
	result = append(result, &MachineVersion{
		IP:          "self",
		VersionValue: &vi,
		Message:     "success",
	})

	query, err := ms.cli.Query(ctx, proto.AdminClusterAPI, client.NewRequest(ctx, `{
		clusterView{
			dataNodes{
				addr
			},
			metaNodes{
				addr
			}
		}
	}`))

	if err != nil {
		return nil, err
	}

	for _, m := range ms.cfg.MasterAddr {
		ip := strings.Split(m, ":")[0]
		get, err := http.Get(fmt.Sprintf("http://%s/version", m))
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "master", err))
			continue
		}

		all, err := ioutil.ReadAll(get.Body)
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "master", err))
			continue
		}

		vi := &proto.VersionValue{}

		if err := json.Unmarshal(all, vi); err != nil {
			result = append(result, ErrMachineVersion(ip, "master", err))
			continue
		}

		result = append(result, &MachineVersion{
			IP:          ip,
			VersionValue: vi,
			Message:     "success",
		})
	}

	dList := query.GetValue("clusterView", "dataNodes").([]interface{})
	for _, d := range dList {
		ip := strings.Split(d.(map[string]interface{})["addr"].(string), ":")[0]
		get, err := http.Get(fmt.Sprintf("http://%s:%s/version", ip, ms.cfg.DataExporterPort))
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "data", err))
			continue
		}

		all, err := ioutil.ReadAll(get.Body)
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "data", err))
			continue
		}

		vi := &proto.VersionValue{}

		if err := json.Unmarshal(all, vi); err != nil {
			result = append(result, ErrMachineVersion(ip, "data", err))
			continue
		}

		result = append(result, &MachineVersion{
			IP:          ip,
			VersionValue: vi,
			Message:     "success",
		})
	}

	mList := query.GetValue("clusterView", "metaNodes").([]interface{})
	for _, m := range mList {
		ip := strings.Split(m.(map[string]interface{})["addr"].(string), ":")[0]
		get, err := http.Get(fmt.Sprintf("http://%s:%s/version", ip, ms.cfg.DataExporterPort))
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "meta", err))
			continue
		}

		all, err := ioutil.ReadAll(get.Body)
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "meta", err))
			continue
		}

		vi := &proto.VersionValue{}

		if err := json.Unmarshal(all, vi); err != nil {
			result = append(result, ErrMachineVersion(ip, "meta", err))
			continue
		}

		result = append(result, &MachineVersion{
			IP:          ip,
			VersionValue: vi,
			Message:     "success",
		})
	}

	return result, nil
}

func (ms *MonitorService) RangeQuery(ctx context.Context, args struct {
	Query string
	Start uint32
	End   uint32
	Step  uint32
}) (string, error) {

	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.cfg.MonitorCluster)

	param := url.Values{}
	param.Set("query", args.Query)
	param.Set("start", strconv.Itoa(int(args.Start)))
	param.Set("end", strconv.Itoa(int(args.End)))
	param.Set("step", strconv.Itoa(int(args.Step)))

	resp, err := http.DefaultClient.Get(ms.cfg.MonitorAddr + "/api/v1/query_range?" + param.Encode())

	if err != nil {
		return "", err
	}

	b, err := ioutil.ReadAll(resp.Body)

	return string(b), nil
}

func (ms *MonitorService) RangeQueryURL(ctx context.Context, args struct {
	Query string
	Start uint32
	End   uint32
	Step  uint32
}) (string, error) {

	_, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return "", err
	}

	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.cfg.MonitorCluster)

	param := url.Values{}
	param.Set("query", args.Query)
	param.Set("start", strconv.Itoa(int(args.Start)))
	param.Set("end", strconv.Itoa(int(args.End)))
	param.Set("step", strconv.Itoa(int(args.Step)))

	return ms.cfg.MonitorAddr + "/api/v1/query_range?" + param.Encode(), nil
}

func (ms *MonitorService) Query(ctx context.Context, args struct {
	Query string
}) (string, error) {

	_, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return "", err
	}

	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.cfg.MonitorCluster)

	param := url.Values{}
	param.Set("query", args.Query)

	resp, err := http.DefaultClient.Get(ms.cfg.MonitorAddr + "/api/v1/query?" + param.Encode())

	if err != nil {
		return "", err
	}

	b, err := ioutil.ReadAll(resp.Body)

	return string(b), nil
}

type FuseClient struct {
	Name     string `json:"__name__"`
	App      string `json:"app"`
	Cluster  string `json:"cluster"`
	Hostip   string `json:"hostip"`
	Instance string `json:"instance"`
	Job      string `json:"job"`
	Monitor  string `json:"monitor"`
	Role     string `json:"role"`
	Service  string `json:"service"`
}

type FuseRecord struct {
	Metric FuseClient `json:"metric"`
	Value  float64    `json:"value"`
}

func (ms *MonitorService) FuseClientList(ctx context.Context, args struct{}) ([]*FuseRecord, error) {

	_, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return nil, err
	}

	query := `up{app="$app", role="fuseclient", cluster="$cluster"}`
	query = strings.ReplaceAll(query, "$cluster", ms.cfg.MonitorCluster)

	param := url.Values{}
	param.Set("query", query)
	param.Set("time", strconv.Itoa(int(time.Now().Unix())))

	resp, err := http.DefaultClient.Get(ms.cfg.MonitorAddr + "/api/v1/query?" + param.Encode())
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(resp.Body)

	result := struct {
		Data struct {
			Result []*struct {
				Metric FuseClient    `json:"metric"`
				Value  []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}{}
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, err
	}

	frs := make([]*FuseRecord, 0, len(result.Data.Result))

	for _, r := range result.Data.Result {
		frs = append(frs, &FuseRecord{
			Metric: r.Metric,
			Value:  r.Value[0].(float64),
		})
	}

	return frs, nil
}

func (ms *MonitorService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()
	query := schema.Query()
	query.FieldFunc("Query", ms.Query)
	query.FieldFunc("RangeQuery", ms.RangeQuery)
	query.FieldFunc("RangeQueryURL", ms.RangeQueryURL)
	query.FieldFunc("_empty", ms.empty)
	query.FieldFunc("FuseClientList", ms.FuseClientList)
	query.FieldFunc("VersionCheck", ms.VersionCheck)

	return schema.MustBuild()
}
