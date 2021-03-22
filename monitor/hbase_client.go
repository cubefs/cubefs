package monitor

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

type HBaseClient struct {
	namespace   string
	cluster     string
	client      gohbase.Client      // operate data
	adminClient gohbase.AdminClient // operate table
}

// 'families' is a map of column family name to its attributes, refer to 'defaultAttributes'
func (hc *HBaseClient) CreateTable(table string, cFamilies []string) (err error) {
	cf := make(map[string]map[string]string, len(cFamilies))
	for _, f := range cFamilies {
		cf[f] = nil
	}
	// pre-split table for reverse scan test of region changes
	// keySplits := [][]byte{[]byte("REVTEST-100"), []byte("REVTEST-200"), []byte("REVTEST-300")}
	createTable := hrpc.NewCreateTable(context.Background(), []byte(table), cf, hrpc.SetCreateNamespace(hc.namespace))
	if err = hc.adminClient.CreateTable(createTable); err != nil {
		return err
	}
	return nil
}

func (hc *HBaseClient) DeleteTable(table string) (err error) {
	disableTable := hrpc.NewDisableTable(context.Background(), []byte(table), hc.namespace)
	if err = hc.adminClient.DisableTable(disableTable); err != nil {
		if !strings.Contains(err.Error(), "TableNotEnabledException") {
			return err
		}
	}
	deleteTable := hrpc.NewDeleteTable(context.Background(), []byte(table), hc.namespace)
	if err = hc.adminClient.DeleteTable(deleteTable); err != nil {
		return err
	}
	return nil
}

func (hc *HBaseClient) ListTables() (tableNames []string, err error) {
	var (
		listTables *hrpc.ListTableNames
		names      []*pb.TableName
	)
	tablePrefix := fmt.Sprintf("%v_%v_", TablePrefix, hc.cluster)
	listTables, err = hrpc.NewListTableNames(
		context.Background(),
		//hrpc.ListRegex(tablePrefix+".*"),
		hrpc.ListNamespace(hc.namespace),
	)
	if err != nil {
		return
	}
	if names, err = hc.adminClient.ListTableNames(listTables); err != nil {
		return
	}
	log.LogDebugf("list tables: num(%v), prefix(%v) namespace(%v)", len(names), tablePrefix, hc.namespace)
	for _, name := range names {
		if strings.HasPrefix(string(name.GetQualifier()), tablePrefix) && string(name.GetNamespace()) == hc.namespace {
			tableNames = append(tableNames, string(name.Qualifier))
		}
	}
	return tableNames, nil
}

// Values maps a ColumnFamily -> Qualifiers -> Values. Example: values = map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte{0}}}
func (hc *HBaseClient) PutData(table, rowKey string, values map[string]map[string][]byte) (err error) {
	var (
		putRequest *hrpc.Mutate
		resp       *hrpc.Result
	)
	table = hc.namespace + ":" + table
	if putRequest, err = hrpc.NewPutStr(context.Background(), table, rowKey, values); err != nil {
		log.LogErrorf("PutData err: NewPutStr err[%v] table[%v] rowKey[%v] values[%v]", err, table, rowKey, values)
		return
	}
	if resp, err = hc.client.Put(putRequest); err != nil {
		log.LogErrorf("PutData err: hbaseClient request err[%v] putRequest[%v] table[%v] rowKey[%v] values[%v]", err, putRequest, table, rowKey, values)
		return
	}
	log.LogDebugf("PutData resp: %v", resp)
	return nil
}

func (hc *HBaseClient) GetEntireData(table, rowKey string) (resp *hrpc.Result, err error) {
	var getRequest *hrpc.Get
	if getRequest, err = hrpc.NewGetStr(context.Background(), table, rowKey); err != nil {
		log.LogErrorf("GetEntireData err: NewGetStr err[%v] table[%v] rowKey[%v]", err, table, rowKey)
		return
	}
	if resp, err = hc.client.Get(getRequest); resp == nil || err != nil {
		log.LogErrorf("GetEntireData err: hbaseClient request err[%v] getRequest[%v] table[%v] rowKey[%v]", err, getRequest, table, rowKey)
		return
	}
	if resp.Cells == nil {
		log.LogDebugf("GetEntireData: no this rowKey[%v] in table[%v]", rowKey, table)
	} else {
		log.LogDebugf("GetData resp: %v", resp)
	}
	return
}

func (hc *HBaseClient) ScanTableWithPrefixKey(tableName, prefixKey string) (results []*hrpc.Result, err error) {
	var scan *hrpc.Scan
	keyFilter := filter.NewPrefixFilter([]byte(prefixKey))
	//tableName = hc.namespace + ":" + tableName
	if scan, err = hrpc.NewScanStr(context.Background(), tableName, hrpc.Filters(keyFilter)); err != nil {
		log.LogErrorf("ScanTableWithPrefixKey err: NewScanStr err[%v] table[%v] prefixKey[%v]", err, tableName, prefixKey)
		return
	}
	scanner := hc.client.Scan(scan)
	for {
		var res *hrpc.Result
		res, err = scanner.Next()
		if err == io.EOF {
			return results, nil
		}
		if err != nil {
			log.LogErrorf("ScanTableWithPrefixKey err: scanner next err[%v] results len[%v] table[%v] prefixKey[%v]", err, len(results), tableName, prefixKey)
			return
		}
		results = append(results, res)
	}
	//return
}
