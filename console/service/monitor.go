package service

import (
	"context"
	"encoding/json"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type MonitorService struct {
	Address       string
	App           string
	Cluster       string
	DashboardAddr string
}

func NewMonitorService(addr, app, cluster, dashboardAddr string) *MonitorService {
	return &MonitorService{
		Address:       addr,
		App:           app,
		Cluster:       cluster,
		DashboardAddr: dashboardAddr,
	}
}

func (fs *MonitorService) empty(ctx context.Context, args struct {
	Empty bool
}) bool {
	return args.Empty
}

func (fs *MonitorService) Dashboard(ctx context.Context, args struct{}) string {
	return fs.DashboardAddr
}

func (ms *MonitorService) RangeQuery(ctx context.Context, args struct {
	Query string
	Start uint32
	End   uint32
	Step  uint32
}) (string, error) {

	args.Query = strings.ReplaceAll(args.Query, "$app", ms.App)
	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.Cluster)

	param := url.Values{}
	param.Set("query", args.Query)
	param.Set("start", strconv.Itoa(int(args.Start)))
	param.Set("end", strconv.Itoa(int(args.End)))
	param.Set("step", strconv.Itoa(int(args.Step)))

	resp, err := http.DefaultClient.Get(ms.Address + "/api/v1/query_range?" + param.Encode())

	if err != nil {
		return "", err
	}

	b, err := io.ReadAll(resp.Body)

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

	args.Query = strings.ReplaceAll(args.Query, "$app", ms.App)
	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.Cluster)

	param := url.Values{}
	param.Set("query", args.Query)
	param.Set("start", strconv.Itoa(int(args.Start)))
	param.Set("end", strconv.Itoa(int(args.End)))
	param.Set("step", strconv.Itoa(int(args.Step)))

	return ms.Address + "/api/v1/query_range?" + param.Encode(), nil
}

func (ms *MonitorService) Query(ctx context.Context, args struct {
	Query string
}) (string, error) {

	_, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return "", err
	}

	args.Query = strings.ReplaceAll(args.Query, "$app", ms.App)
	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.Cluster)

	param := url.Values{}
	param.Set("query", args.Query)

	resp, err := http.DefaultClient.Get(ms.Address + "/api/v1/query?" + param.Encode())

	if err != nil {
		return "", err
	}

	b, err := io.ReadAll(resp.Body)

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
	query = strings.ReplaceAll(query, "$app", ms.App)
	query = strings.ReplaceAll(query, "$cluster", ms.Cluster)

	param := url.Values{}
	param.Set("query", query)
	param.Set("time", strconv.Itoa(int(time.Now().Unix())))

	resp, err := http.DefaultClient.Get(ms.Address + "/api/v1/query?" + param.Encode())
	if err != nil {
		return nil, err
	}

	b, err := io.ReadAll(resp.Body)

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
	query.FieldFunc("Dashboard", ms.Dashboard)
	query.FieldFunc("FuseClientList", ms.FuseClientList)
	return schema.MustBuild()
}
