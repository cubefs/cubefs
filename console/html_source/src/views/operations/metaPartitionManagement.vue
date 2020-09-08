<template>
  <div class="mp">
    <div>
      <div class="partition-right">
        <span class="pr10">ID</span>
        <el-input v-model="searchVal" placeholder style="width: 255px;"></el-input>
        <el-button type="primary" class="ml5" @click="queryMetaPartitionList" >{{ $t('chubaoFS.tools.Search') }}</el-button>
      </div>
    </div>
    <div class="data-block" v-loading="resData.loading">
      <el-table :data="resData.resLists" class="mt10" style="width: 100%">
        <el-table-column prop="partitionID" :label="$t('chubaoFS.operations.MetaPartitionManagement.PartitionID')"></el-table-column>
        <el-table-column prop="dentryCount" :label="$t('chubaoFS.operations.MetaPartitionManagement.dentryCount')"></el-table-column>
        <el-table-column prop="isRecover" :label="$t('chubaoFS.operations.MetaPartitionManagement.isRecover')"></el-table-column>
        <el-table-column prop="replicaNum" :label="$t('chubaoFS.operations.MetaPartitionManagement.replicaNum')"></el-table-column>
        <el-table-column prop="status" :label="$t('chubaoFS.operations.MetaPartitionManagement.status')"></el-table-column>
        <el-table-column prop="missNodes" :label="$t('chubaoFS.operations.MetaPartitionManagement.missNodes')"></el-table-column>
        <el-table-column prop="start" :label="$t('chubaoFS.operations.MetaPartitionManagement.start')"></el-table-column>
        <el-table-column prop="end" :label="$t('chubaoFS.operations.MetaPartitionManagement.end')"></el-table-column>
        <el-table-column prop :label="$t('chubaoFS.tools.Actions')">
          <template slot-scope="scope">
            <el-dropdown trigger="click" size="medium">
              <span class="el-dropdown-link">
                {{$t('chubaoFS.tools.Replica')}}
                <i class="el-icon-arrow-down el-icon--right"></i>
              </span>
              <el-dropdown-menu slot="dropdown" class="my-dropdown">
                <el-dropdown-item @click.native="openDialog('addReplica', scope.row)" >{{$t('chubaoFS.tools.Add')}}</el-dropdown-item>
                <el-dropdown-item @click.native="openDialog('deleteReplica', scope.row)" >{{$t('chubaoFS.tools.Delete')}}</el-dropdown-item>
                <el-dropdown-item @click.native="openDialog('decommissionReplica', scope.row)" >{{$t('chubaoFS.tools.Decommission')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </template>
        </el-table-column>
      </el-table>
      <div class="clearfix mt20">
        <el-pagination
          class="fr"
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
          :page-sizes="resData.page.pageSizes"
          :page-size="resData.page.pageSize"
          layout="sizes, prev, pager, next"
          :total="resData.page.totalRecord"
        ></el-pagination>
        <span class="fr page-tips pr10">{{ $t('chubaoFS.commonTxt.eachPageShows') }}</span>
      </div>
    </div>
    <!-- Add Replica -->
    <el-dialog
      :title="$t('chubaoFS.operations.MetaPartitionManagement.AddReplica')"
      :visible.sync="addReplicaDialog"
      width="35%"
      @close="closeDialog('addReplica')"
    >
      <el-form :label-position="labelPosition" :model="ReplicaForm">
        <el-form-item :label="$t('chubaoFS.operations.MetaPartitionManagement.PartitionID')">
          <el-input type="text" v-model="ReplicaForm.partitionID" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.operations.MetaPartitionManagement.Hosts')">
          <el-select v-model="ReplicaForm.addr" style="width: 100%;">
            <el-option v-for="mn in metaNodes" :value="mn.addr" :label="mn.addr +' num:['+mn.toMetaNode.metaPartitionCount+ '] free:['+(mn.toMetaNode.total-mn.toMetaNode.used)+']'" :key="mn.addr"></el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="closeDialog('addReplica')">{{$t('chubaoFS.tools.Cancel')}}</el-button>
        <el-button type="primary" @click="addReplica">{{$t('chubaoFS.tools.Yes')}}</el-button>
      </span>
    </el-dialog>
    <!-- Delete Replica -->
    <el-dialog
      :title="$t('chubaoFS.operations.MetaPartitionManagement.DeleteReplica')"
      :visible.sync="deleteReplicaDialog"
      width="35%"
      @close="closeDialog('deleteReplica')"
    >
      <el-form :label-position="labelPosition" :model="ReplicaForm">
        <el-form-item :label="$t('chubaoFS.operations.MetaPartitionManagement.PartitionID')">
          <el-input type="text" v-model="ReplicaForm.partitionID" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.operations.MetaPartitionManagement.Hosts')">
          <el-select v-model="ReplicaForm.addr" style="width: 100%;">
            <el-option v-for="host in currentHosts" :value="host" :label="host" :key="host"></el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="closeDialog('deleteReplica')">{{$t('chubaoFS.tools.Cancel')}}</el-button>
        <el-button type="primary" @click="deleteReplica">{{$t('chubaoFS.tools.Yes')}}</el-button>
      </span>
    </el-dialog>
    <!-- decommission Replica -->
    <el-dialog
      :title="$t('chubaoFS.operations.MetaPartitionManagement.DecommissionReplica')"
      :visible.sync="decommissionReplicaDialog"
      width="35%"
      @close="closeDialog('decommissionReplica')"
    >
      <el-form :label-position="labelPosition" :model="ReplicaForm">
        <el-form-item :label="$t('chubaoFS.operations.MetaPartitionManagement.PartitionID')">
          <el-input type="text" v-model="ReplicaForm.partitionID" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.operations.MetaPartitionManagement.Hosts')">
          <el-select v-model="ReplicaForm.addr" style="width: 100%;">
            <el-option v-for="host in currentHosts" :value="host" :label="host" :key="host"></el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="closeDialog('decommissionReplica')">{{$t('chubaoFS.tools.Cancel')}}</el-button>
        <el-button type="primary" @click="decommissionReplica">{{$t('chubaoFS.tools.Yes')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import baseGql from "../../graphql/operations";
import { date2Str, time2Str } from "../../utils/dateTime.js";
import { formatStatus } from "../../utils/string.js";
export default {
  name: "metaPartitionManagement",
  data() {
    return {
      labelPosition: "top",
      metaPartitionList :[],
      resData: {
        loading: true,
        page: {
          pageSizes: [10, 20, 30, 40],
          pageNo: 1,
          pageSize: 10,
          totalRecord: 0,
          totalPage: 1,
        },
        resLists: [],
      },
      searchVal: "",
      addReplicaDialog:false,
      deleteReplicaDialog:false,
      decommissionReplicaDialog:false, 
      currentHosts:[],
      metaNodes:[],
      ReplicaForm: {
        partitionID:0,
        addr:""
      }
    };
  },
  methods: {
    handleSizeChange(val) {
      this.resData.page.pageSize = val;
      this.resData.page.pageNo = 1;
      this.handleCurrentChange(1);
    },
    handleCurrentChange(val) {
      this.resData.page.pageNo = val;
      const start = (val - 1) * this.resData.page.pageSize;
      const end = val * this.resData.page.pageSize;
      this.resData.resLists = this.metaPartitionList.slice(start, end);
    },
    queryMetaPartitionList() {
      this.resData.loading = true;
      const variables = {
        keyword: "",
        userID: this.userID,
        num: 10000,
      };
      this.apollo
        .query(this.url.cluster, baseGql.queryMetaPartitionList, variables)
        .then((res) => {
          this.resData.loading = false;
          if (res.data) {
            let finalMetaPartitionList = [];
            if (this.searchVal && this.searchVal.trim() !== "") {
              res.data.metaPartitionList.forEach((item) => {
                if (item.partitionID.toString() == this.searchVal.trim()) {
                  finalMetaPartitionList.push(item);
                }
              });
            } else {
              finalMetaPartitionList = res.data.metaPartitionList;
            }
            // 按创建时间倒序排列
            finalMetaPartitionList.sort(function (a, b) {
              let errA = a.peers.length != a.replicaNum ;
              let errB = b.peers.length != b.replicaNum ;

              if (!errA && !errB){
                return 0;
              }

              if(!errA){
                return 1;
              }

              if(!errB){
                return -1;
              }
              return a.partitionID - b.partitionID;
            });
            this.metaPartitionList = finalMetaPartitionList;
            this.resData.page.totalRecord = res.data.metaPartitionList.length;
            this.handleCurrentChange(1);
            this.totalCapacity = 0;
            this.metaPartitionList.forEach((item) => {
              item.status = formatStatus(item.status);
              item.isRecover = item.isRecover.toString();
              item.replicaNum = item.replicaNum +"/"+ item.peers.length ;
              if(item.missNodes == undefined || item.missNodes == null || item.missNodes.length ==0){
                item.missNodes = "none";
              }else{
                item.missNodes = item.missNodes.toString();
              }
            });
          } else {
            this.$message.error(res.message);
          }
        })
        .catch((error) => {
          this.resData.loading = false;
          console.log(error);
        });
    },
    queryMetaNodeList() {
      this.resData.loading = true;
      const variables = {
        userID: this.userID,
      };
      this.apollo
        .query(this.url.cluster, baseGql.queryMetaNodeAddrList, variables)
        .then((res) => {
          this.resData.loading = false;

          let filterMeta = {}

          this.currentHosts.forEach((a)=>{
            filterMeta[a] = true 
          })

          if (res.data) {
            let list = [];
            res.data.clusterView.metaNodes.forEach((item) => {
              if (!filterMeta[item.addr] && item.status){
                list.push(item);
              }
            });
            // 按创建时间倒序排列
            list.sort(function (a, b) {
              return b.toMetaNode.metaPartitionCount - a.toMetaNode.metaPartitionCount;
            });
            this.metaNodes = list;
          } else {
            this.$message.error(res.message);
          }
        })
        .catch((error) => {
          this.resData.loading = false;
          console.log(error);
        });
    },
    openDialog(tag, row) {
      if (tag === "addReplica") {
        this.addReplicaDialog = true;
        this.ReplicaForm.partitionID = row.partitionID;
        this.currentHosts = row.hosts ;
        this.ReplicaForm.addr = "";
        this.queryMetaNodeList();
      }else if (tag == "deleteReplica"){
        this.deleteReplicaDialog = true;
        this.ReplicaForm.partitionID = row.partitionID;
        this.currentHosts = row.hosts ;
        this.ReplicaForm.addr = "";
      }else if (tag == "decommissionReplica"){
        this.decommissionReplicaDialog = true;
        this.ReplicaForm.partitionID = row.partitionID;
        this.currentHosts = row.hosts ;
        this.ReplicaForm.addr = "";
      }
    },
    closeDialog(tag) {
      if (tag === "addReplica") {
        this.addReplicaDialog = false;
        this.ReplicaForm.addr = "";
        this.ReplicaForm.partitionID = 0 ;
      }else if (tag == "deleteReplica"){
        this.deleteReplicaDialog = false;
        this.ReplicaForm.addr = "";
        this.ReplicaForm.partitionID = 0 ;
      }else if (tag == "decommissionReplica"){
        this.decommissionReplicaDialog = false;
        this.ReplicaForm.addr = "";
        this.ReplicaForm.partitionID = 0 ;
      }
    },
    addReplica(){
      const variables = {
        partitionID: this.ReplicaForm.partitionID,
        addr: this.ReplicaForm.addr,
      };
      this.apollo
            .mutation(this.url.cluster, baseGql.addMetaReplica, variables)
            .then((res) => {
              if (res.code === 200) {
                this.queryMetaPartitionList();
                this.$message({
                  message: this.$t("chubaoFS.message.Success"),
                  type: "success",
                });
                this.closeDialog('addReplica');
              } else {
                this.$message.error(res.message);
              }
            });
      
    },
    deleteReplica(){
      const variables = {
        partitionID: this.ReplicaForm.partitionID,
        addr: this.ReplicaForm.addr,
      };
      this.apollo
            .mutation(this.url.cluster, baseGql.deleteMetaReplica, variables)
            .then((res) => {
              if (res.code === 200) {
                this.queryMetaPartitionList();
                this.$message({
                  message: this.$t("chubaoFS.message.Success"),
                  type: "success",
                });
                this.closeDialog('deleteReplica');
              } else {
                this.$message.error(res.message);
              }
            });
    },
    decommissionReplica(){
      const variables = {
        partitionID: this.ReplicaForm.partitionID,
        addr: this.ReplicaForm.addr,
      };
      this.apollo
            .mutation(this.url.cluster, baseGql.decommissionMetaPartition, variables)
            .then((res) => {
              if (res.code === 200) {
                this.queryMetaPartitionList();
                this.$message({
                  message: this.$t("chubaoFS.message.Success"),
                  type: "success",
                });
                this.closeDialog('decommissionReplica');
              } else {
                this.$message.error(res.message);
              }
            });
    },
  },
  mounted() {
    // this.()
  },
};
</script>

<style scoped>
.partition-left {
  display: inline-block;
}
.partition-right {
  display: inline-block;
  float: right;
}
.partition-right span {
  margin-right: 15px;
}
.partition-summary {
  font-family: Helvetica;
  font-size: 14px;
  line-height: 17px;
  color: rgba(51, 51, 51, 1);
}
</style>
<style>
.el-dropdown-link {
  cursor: pointer;
  color: #466be4;
  font-size: 13px;
}
.my-dropdown.el-dropdown-menu {
  width: 93px;
}
.my-dropdown .el-dropdown-menu__item {
  text-align: left;
  font-size: 12px;
}
.my-dropdown .el-dropdown-menu__item--divided {
  border-color: #ececec;
}
.el-form-item__error {
  position: unset !important;
}
</style>
