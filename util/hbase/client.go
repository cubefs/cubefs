package hbase

import (
	"sort"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/cubefs/cubefs/util/log"
)

// todo open hbase client once
func OpenHBaseClient(thriftAddr string) (client *THBaseServiceClient, err error) {
	var transport *thrift.TSocket
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	if transport, err = thrift.NewTSocket(thriftAddr); err != nil {
		log.LogErrorf("NewHBaseClient failed: err(%v) addr(%v)", err, thriftAddr)
		return
	}
	client = NewTHBaseServiceClientFactory(transport, protocolFactory)
	if err = transport.Open(); err != nil {
		log.LogErrorf("hbase open transport failed: err(%v) addr(%v)", err, thriftAddr)
		return
	}
	return
}

func (client *THBaseServiceClient) CloseHBaseClient() (err error) {
	if err = client.Transport.Close(); err != nil {
		log.LogErrorf("hbase open transport failed: err(%v)", err)
		return
	}
	return
}

func (client *THBaseServiceClient) PutData(namespace, table, rowKey string, columns []*TColumnValue) (err error) {
	tput := &TPut{
		Row:          []byte(rowKey),
		ColumnValues: columns,
	}
	tableName := namespace + ":" + table
	if err = client.Put([]byte(tableName), tput); err != nil {
		return err
	}
	return
}

func (client *THBaseServiceClient) PutMultiData(namespace, table string, dataMap map[string][]*TColumnValue) (err error) {
	tputs := make([]*TPut, 0)
	for rowKey, columns := range dataMap {
		tput := &TPut{
			Row:            []byte(rowKey),
			ColumnValues:   columns,
		}
		tputs = append(tputs, tput)
	}
	tableName := namespace + ":" + table
	if err = client.PutMultiple([]byte(tableName), tputs); err != nil {
		return err
	}
	return
}

func (client *THBaseServiceClient) GetData(namespace, table, rowKey string) (r *TResult_, err error) {
	tget := &TGet{
		Row:	[]byte(rowKey),
	}
	tableName := namespace + ":" + table
	return client.Get([]byte(tableName), tget)
}

func (client *THBaseServiceClient) CreateHBaseTable(namespace, tableName string, cFamilies []string, splitRegions []string) (err error) {
	cFamiliesDesc := make([]*TColumnFamilyDescriptor, 0)
	for _, cf := range cFamilies {
		cfDesc := &TColumnFamilyDescriptor{
			Name: []byte(cf),
		}
		cFamiliesDesc = append(cFamiliesDesc, cfDesc)
	}
	desc := &TTableDescriptor{
		TableName:  &TTableName{
			Ns:        []byte(namespace),
			Qualifier: []byte(tableName),
		},
		Columns:    cFamiliesDesc,
	}
	splitKeys := make([][]byte, len(splitRegions))
	sort.Strings(splitRegions)
	for i, splitRegion := range splitRegions {
		splitKeys[i] = []byte(splitRegion)
	}
	if err = client.CreateTable(desc, splitKeys); err != nil {
		return err
	}
	return
}

func (client *THBaseServiceClient) DeleteHBaseTable(namespace, tableName string) (err error) {
	desc := &TTableName{
		Ns:        []byte(namespace),
		Qualifier: []byte(tableName),
	}
	if err = client.DisableTable(desc); err != nil {
		return err
	}
	if err = client.DeleteTable(desc); err != nil {
		return err
	}
	return
}

func (client *THBaseServiceClient) ListHBaseTables(namsespace string) (tables []*TTableName, err error) {
	return client.GetTableNamesByNamespace(namsespace)
}