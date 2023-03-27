package convertnode

import "testing"

var dbConf = DBConfig{
	Path:         "",
	Config:       "charset=utf8&parseTime=True&loc=Local",
	Dbname:       "",
	Username:     "",
	Password:     "",
	MaxIdleConns: 10,
	MaxOpenConns: 100,
	LogMode:      false,
	LogZap:       "",
}

func TestDBInfo_PutClusterInfoToDB(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	if err := db.PutClusterInfoToDB("testConvertNodeDB", []string{"127.0.0.1:7001"}); err != nil {
		t.Errorf("put to data base failed:%v\n", err)
		t.FailNow()
	}
}

func TestDBInfo_UpdateClusterInfoToDB(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	if err := db.UpdateClusterInfoToDB("testConvertNodeDB", []string{"127.0.0.1:7001", "127.0.0.1:7002"}); err != nil {
		t.Errorf("update cluster nodes failed:%v\n", err)
		t.FailNow()
	}
}

func TestDBInfo_LoadAllClusterInfoFromDB(t *testing.T) {
	db := &DBInfo{
		Config: dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	clusters, err := db.LoadAllClusterInfoFromDB()
	if err != nil {
		t.Errorf("load all cluster info failed:%v\n", err)
		return
	}

	for _, cluster := range clusters {
		if cluster.ClusterName == "testConvertNodeDB" {
			t.Logf("cluster(testConvertNodeDB) found\n")
			return
		}
	}

	t.Errorf("cluster(testConvertNodeDB) not founc!\n")
	t.FailNow()
}

func TestDBInfo_DeleteClusterInfoFromDB(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	if err := db.DeleteClusterInfoFromDB("testConvertNodeDB"); err != nil {
		t.Errorf("delete cluster info failed:%v\n", err)
		return
	}

	clusters, err := db.LoadAllClusterInfoFromDB()
	if err != nil {
		t.Errorf("load all cluster info failed:%v\n", err)
		return
	}

	for _, cluster := range clusters {
		if cluster.ClusterName == "testConvertNodeDB" {
			t.Errorf("cluster(testConvertNodeDB) found, result mismatch, expect:not found, actual:found\n")
			t.FailNow()
			return
		}
	}
}

func TestDBInfo_PutTaskInfoToDB(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	task := &TaskInfo{
		ClusterName:          "testConvertNodeDB",
		VolumeName:           "testDB",
		SelectedMP:           "1,2,3",
		FinishedMP:           "4",
		RunningMP:            5,
		Layout:               "40,100",
		Process:              0,
		TaskStage:            3,
		MPConvertStage:       1,
		SelectedReplNodeAddr: "127.0.0.1:7001",
		OldNodeAddr:          "127.0.0.2:7001",
	}
	if err := db.PutTaskInfoToDB(task); err != nil {
		t.Errorf("put task info to database failed:%v\n", err)
		return
	}
}

func TestDBInfo_LoadAllTaskInfoFromDB(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	tasks, err := db.LoadAllTaskInfoFromDB()
	if err != nil {
		t.Errorf("load all task info failed:%v\n", err)
		return
	}

	for _, task := range tasks {
		if task.ClusterName == "testConvertNodeDB" && task.VolumeName == "testDB" {
			t.Logf("task(testConvertNodeDB_testDB) found\n")
			return
		}
	}
	t.Errorf("task(testConvertNodeDB_testDB) not found\n")
	t.FailNow()
}

func TestDBInfo_DeleteTaskInfoFromDB(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	if err := db.DeleteTaskInfoFromDB("testConvertNodeDB", "testDB"); err != nil {
		t.Errorf("delete task info from database failed:%v", err)
		return
	}

	tasks, err := db.LoadAllTaskInfoFromDB()
	if err != nil {
		t.Errorf("load all task info failed:%v\n", err)
		return
	}

	for _, task := range tasks {
		if task.ClusterName == "testConvertNodeDB" && task.VolumeName == "testDB" {
			t.Errorf("task(testConvertNodeDB_testDB) found, expect:not found\n")
			t.FailNow()
			return
		}
	}
}

func TestDBInfo_PutIpAddrToDataBase(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	if err := db.PutIpAddrToDataBase("127.0.0.1:7002"); err != nil {
		t.Errorf("put ip addr to database failed:%v\n", err)
		return
	}
}

func TestDBInfo_LoadIpAddrToDataBase(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	result, err := db.LoadIpAddrFromDataBase()
	if err != nil {
		t.Errorf("load ip addr from database failed:%v\n", err)
		return
	}

	for _, ip := range result {
		if ip.IpAddr == "127.0.0.1:7002" {
			return
		}
	}
	t.Errorf("ip(127.0.0.1:7002) not found\n")
	t.FailNow()
}

func TestDBInfo_ClearIpAddrInDataBase(t *testing.T) {
	db := &DBInfo{
		Config:    dbConf,
	}

	if err := db.Open(); err != nil {
		t.Errorf("open database(%s:%s) failed:%v\n", db.Config.Path, db.Config.Dbname, err)
		return
	}
	defer db.ReleaseDBHandle()

	if err := db.ClearIpAddrInDataBase("127.0.0.1:7002"); err != nil {
		t.Errorf("clear ip addr(127.0.0.1:7002) failed:%v", err)
		t.FailNow()
		return
	}

	result, err := db.LoadIpAddrFromDataBase()
	if err != nil {
		t.Errorf("load ip addr from database failed:%v\n", err)
		return
	}

	for _, ip := range result {
		if ip.IpAddr == "127.0.0.1:7002" {
			t.Errorf("ip(127.0.0.1:7002) found, expect:not found\n")
			t.FailNow()
		}
	}

}
