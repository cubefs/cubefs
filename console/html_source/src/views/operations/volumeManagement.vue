<template>
  <div class="volume">
    <div>
      <div class="volume-left">
        <el-button
          type="primary"
          @click="createVolumeDialog = true"
        >{{$t('chubaoFS.operations.VolumeManagement.CreateVolume')}}</el-button>
      </div>
      <div class="volume-right">
        <span class="pr10">{{$t('chubaoFS.operations.VolumeManagement.VolumeName')}}</span>
        <el-input v-model="searchVal" placeholder style="width: 255px;"></el-input>
        <el-button
          type="primary"
          class="ml5"
          @click="queryVolumeList"
        >{{ $t('chubaoFS.tools.Search') }}</el-button>
      </div>
    </div>
    <p class="volume-summary mt20">{{$t('chubaoFS.operations.VolumeManagement.VolumeNumber')}} : {{this.resData.page.totalRecord}} {{$t('chubaoFS.operations.VolumeManagement.TotalCapacity')}} : {{totalCapacity}}G</p>
    <div class="data-block" v-loading="resData.loading">
      <el-table :data="resData.resLists" class="mt10" style="width: 100%">
        <el-table-column prop="name" :label="$t('chubaoFS.operations.VolumeManagement.Name')"></el-table-column>
        <el-table-column prop="capacityStr" :label="$t('chubaoFS.operations.VolumeManagement.TotalCapacity')"></el-table-column>
        <el-table-column prop="occupiedStr" :label="$t('chubaoFS.operations.VolumeManagement.Used')"></el-table-column>
        <el-table-column prop="occupiedPercent" :label="$t('chubaoFS.operations.VolumeManagement.UsedRate')"></el-table-column>
        <el-table-column prop="inodeCount" :label="$t('chubaoFS.operations.VolumeManagement.InodeCount')"></el-table-column>
        <el-table-column prop="dpReplicaNum" :label="$t('chubaoFS.operations.VolumeManagement.Replications')"></el-table-column>
        <el-table-column prop="status" :label="$t('chubaoFS.operations.VolumeManagement.Status')" width="80"></el-table-column>
        <el-table-column prop="toSimpleVolView.dpCnt" :label="$t('chubaoFS.operations.VolumeManagement.TotalDataPartition')" width="85"></el-table-column>
        <el-table-column prop="toSimpleVolView.rwDpCnt" :label="$t('chubaoFS.operations.VolumeManagement.AvailableDataPartition')" width="110"></el-table-column>
        <el-table-column prop="toSimpleVolView.mpCnt" :label="$t('chubaoFS.operations.VolumeManagement.TotalMetapartition')" width="130"></el-table-column>
        <el-table-column prop="owner" :label="$t('chubaoFS.operations.VolumeManagement.OwnerID')"></el-table-column>
        <el-table-column
          prop="createTime"
          :label="$t('chubaoFS.operations.VolumeManagement.CreateTime')"
          width="145"
        ></el-table-column>
        <el-table-column prop :label="$t('chubaoFS.tools.Actions')">
          <template slot-scope="scope">
            <el-dropdown trigger="click" size="medium">
              <span class="el-dropdown-link">
                {{$t('chubaoFS.tools.Actions')}}
                <i class="el-icon-arrow-down el-icon--right"></i>
              </span>
              <el-dropdown-menu slot="dropdown" class="my-dropdown">
                <el-dropdown-item
                  @click.native="openDialog('extend', scope.row)"
                >{{$t('chubaoFS.tools.Extend')}}</el-dropdown-item>
                <el-dropdown-item
                  @click.native="openDialog('permission', scope.row)"
                >{{$t('chubaoFS.tools.Permission')}}</el-dropdown-item>
                <el-dropdown-item
                  @click.native="openDialog('edit', scope.row)"
                >{{$t('chubaoFS.tools.Edit')}}</el-dropdown-item>
                <el-dropdown-item
                  @click.native="deleteVolume(scope.row)"
                >{{$t('chubaoFS.tools.Delete')}}</el-dropdown-item>
                <el-dropdown-item
                  @click.native="openDialog('notes', scope.row)"
                >{{$t('chubaoFS.tools.Comments')}}</el-dropdown-item>
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
    <!-- Create Volume -->
    <el-dialog
      :title="$t('chubaoFS.operations.VolumeManagement.CreateVolume')"
      :visible.sync="createVolumeDialog"
      width="35%"
      @close="closeDialog('create')"
    >
      <el-form
        :label-position="labelPosition"
        ref="createVolumeForm"
        :model="createVolumeForm"
        :rules="rules"
      >
        <el-form-item
          :label="$t('chubaoFS.operations.VolumeManagement.VolumeName')"
          prop="name"
          :rules="[
            { required: true, message: $t('chubaoFS.volume.EnterNameTip'), trigger: 'blur' },
            { validator: checkVolName, message: $t('chubaoFS.volume.NameRulesTip'), trigger: 'blur' }
          ]"
        >
          <el-input v-model="createVolumeForm.name"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('chubaoFS.operations.VolumeManagement.TotalCapacity')"
          prop="totalCapacity"
          :rules="[
            { required: true, message: $t('chubaoFS.volume.EnterCapacityTip'), trigger: 'blur' },
            { validator: checkNumber, message: $t('chubaoFS.volume.CapacityRules'), trigger: 'blur' }
          ]"
        >
          <el-input v-model="createVolumeForm.totalCapacity"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('chubaoFS.operations.VolumeManagement.OwnerID')"
          prop="owner"
          :rules="[
            { required: true, message: $t('chubaoFS.operations.VolumeManagement.ownerMsg'), trigger: 'blur' },
            { validator: checkOwnerId, message: $t('chubaoFS.operations.VolumeManagement.checkOwnerIdTip'), trigger: 'blur' }
          ]"
        >
          <el-input v-model="createVolumeForm.owner"></el-input>
        </el-form-item>

        <el-form-item :label="$t('chubaoFS.operations.VolumeManagement.ZoneName')" prop="zoneName">
          <el-input v-model="createVolumeForm.zoneName"></el-input>
        </el-form-item>

        <!--        <el-form-item :label="$t('chubaoFS.operations.VolumeManagement.Replications')" prop="dpReplicaNum">-->
        <!--          <el-select v-model="createVolumeForm.dpReplicaNum" style="width: 100%;">-->
        <!--            <el-option v-for="item in dpReplicaNumList" :value="item" :label="item" :key="item"></el-option>-->
        <!--          </el-select>-->
        <!--        </el-form-item>-->
        <el-form-item :label="$t('chubaoFS.tools.Comments')" prop="notes">
          <el-input type="textarea" v-model="createVolumeForm.notes"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="closeDialog('create')">{{$t('chubaoFS.tools.Cancel')}}</el-button>
        <el-button
          type="primary"
          @click="createVolume('createVolumeForm')"
        >{{$t('chubaoFS.tools.Create')}}</el-button>
      </span>
    </el-dialog>
    <!-- Edit Volume -->
    <el-dialog
      :title="$t('chubaoFS.operations.VolumeManagement.EditVolume')"
      :visible.sync="editVolumeDialog"
      width="35%"
    >
      <el-form :label-position="labelPosition" :model="editVolumeForm">
        <!--        <el-form-item label="$t('chubaoFS.operations.VolumeManagement.Replications')">-->
        <!--          <el-select v-model="editVolumeForm.dpReplicaNum" style="width: 100%;">-->
        <!--            <el-option v-for="item in dpReplicaNumList" :value="item" :label="item" :key="item"></el-option>-->
        <!--          </el-select>-->
        <!--        </el-form-item>-->
        <el-form-item :label="$t('chubaoFS.tools.Comments')">
          <el-input type="textarea" v-model="desContent"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="editVolumeDialog = false">{{$t('chubaoFS.tools.Cancel')}}</el-button>
        <el-button type="primary" @click="updateVolume">{{$t('chubaoFS.tools.Update')}}</el-button>
      </span>
    </el-dialog>
    <!-- Notes -->
    <el-dialog :title="$t('chubaoFS.tools.Comments')" :visible.sync="notesDialog" width="35%">
      <div class="pl10">{{description}}</div>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="notesDialog = false">OK</el-button>
      </span>
    </el-dialog>
    <!-- Volume Extension -->
    <el-dialog
      :title="$t('chubaoFS.operations.VolumeManagement.IncreaseVolumeCapacity')"
      :visible.sync="volumeExtensionDialog"
      width="35%"
      @close="closeDialog('extend')"
    >
      <el-form :label-position="labelPosition" :model="editVolumeForm">
        <el-form-item :label="$t('chubaoFS.operations.VolumeManagement.VolumeName')">
          <el-input type="text" v-model="editVolumeForm.name" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.operations.VolumeManagement.CurrentCapacity')">
          <el-input type="text" v-model="editVolumeForm.capacity" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.operations.VolumeManagement.Extendto')">
          <el-input type="text" v-model="extendCapacity"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="closeDialog('extend')">{{$t('chubaoFS.tools.Cancel')}}</el-button>
        <el-button type="primary" @click="extendVolume">{{$t('chubaoFS.tools.Extend')}}</el-button>
      </span>
    </el-dialog>
    <!-- Permission of Volume_Spark -->
    <el-dialog
      :title="$t('chubaoFS.operations.VolumeManagement.PermissionofVolume_Spark')"
      :visible.sync="permissionDialog"
      width="35%"
    >
      <el-button
        type="primary"
        @click="openGrantPermission"
      >{{$t('chubaoFS.operations.VolumeManagement.GrantPerimssion')}}</el-button>
      <el-table :data="permissionList" class="mt10" style="width: 100%">
        <el-table-column type="index" label="#"></el-table-column>
        <el-table-column prop="userID" :label="$t('chubaoFS.commonAttr.UserName')"></el-table-column>
        <el-table-column prop="access" :label="$t('chubaoFS.operations.VolumeManagement.Access')"></el-table-column>
        <el-table-column prop :label="$t('chubaoFS.tools.Operations')">
          <template slot-scope="scope">
            <el-button
              type="text"
              class="text-btn"
              @click="openEditPermission(scope.row)"
            >{{$t('chubaoFS.tools.Edit')}}</el-button>
            <el-button
              type="text"
              class="text-btn"
              @click="deletePermission(scope.row)"
            >{{$t('chubaoFS.tools.Remove')}}</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>
    <!-- Grant Permission -->
    <el-dialog
      :title="$t('chubaoFS.operations.VolumeManagement.GrantPermissionto')"
      :visible.sync="grantPermissionDialog"
      width="35%"
      @close="closeDialog('grantPer')"
    >
      <el-form
        :label-position="labelPosition"
        ref="grantPermissionForm"
        :model="grantPermissionForm"
        :rules="permissionRules"
      >
        <el-form-item :label="$t('chubaoFS.commonAttr.UserName')" prop="userName">
          <el-input v-model="grantPermissionForm.userName"></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.operations.VolumeManagement.Access')" prop="access">
          <el-select v-model="grantPermissionForm.access" style="width: 100%;">
            <el-option v-for="(val,key) in accessList" :value="key" :label="val" :key="key"></el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="closeDialog('grantPer')">{{ $t('chubaoFS.tools.No') }}</el-button>
        <el-button
          type="primary"
          @click="grantPermission('grantPermissionForm')"
        >{{ $t('chubaoFS.tools.Yes') }}</el-button>
      </span>
    </el-dialog>
    <!-- Edit Permission -->
    <el-dialog
      :title="$t('chubaoFS.operations.VolumeManagement.EditUserAccess')"
      :visible.sync="editPermissionDialog"
      width="35%"
    >
      <el-form
        :label-position="labelPosition"
        ref="grantPermissionForm"
        :model="editPermissionForm"
      >
        <el-form-item :label="$t('chubaoFS.commonAttr.UserName')" prop="userName">
          <el-input v-model="editPermissionForm.userID" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.operations.VolumeManagement.Access')" prop="access">
          <el-select v-model="editPermissionForm.access" style="width: 100%;">
            <el-option v-for="(val,key) in accessList" :value="key" :label="val" :key="key"></el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="closeDialog('editPer')">{{ $t('chubaoFS.tools.No') }}</el-button>
        <el-button type="primary" @click="editPermission">{{ $t('chubaoFS.tools.Yes') }}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import baseGql from "../../graphql/operations";
import { date2Str, time2Str } from "../../utils/dateTime.js";
export default {
  name: "volumeManagement",
  data() {
    return {
      labelPosition: "top",
      searchVal: "",
      volumeList: [],
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
      totalCapacity: null,
      createVolumeDialog: false,
      notesDialog: false,
      editVolumeDialog: false,
      volumeExtensionDialog: false,
      permissionDialog: false,
      grantPermissionDialog: false,
      editPermissionDialog: false,
      createVolumeForm: {
        name: "",
        totalCapacity: null,
        dpReplicaNum: null,
        owner: "",
        zoneName: "",
        notes: "",
      },
      dpReplicaNumList: ["2", "3"],
      extendCapacity: null,
      rules: {
        dpReplicaNum: [
          {
            required: true,
            message: this.$t(
              "chubaoFS.operations.VolumeManagement.dpReplicaNumMsg"
            ),
            trigger: "change",
          },
        ],
        owner: [
          {
            required: true,
            message: this.$t("chubaoFS.operations.VolumeManagement.ownerMsg"),
            trigger: "blur",
          },
        ],
        notes: [
          {
            required: true,
            message: this.$t("chubaoFS.operations.VolumeManagement.notesMsg"),
            trigger: "blur",
          },
        ],
      },
      editVolumeForm: {},
      permissionList: [],
      grantPermissionForm: {
        userName: "",
        access: "",
      },
      editPermissionForm: {},
      volName: "",
      accessList: this.$t("chubaoFS.accessList"),
      permissionRules: {
        userName: [
          {
            required: true,
            message: this.$t(
              "chubaoFS.operations.VolumeManagement.userNameMsg"
            ),
            trigger: "blur",
          },
        ],
        access: [
          {
            required: true,
            message: this.$t("chubaoFS.operations.VolumeManagement.accessMsg"),
            trigger: "change",
          },
        ],
      },
      permissionItem: null,
      description: "",
      desContent: "",
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
      this.resData.resLists = this.volumeList.slice(start, end);
    },
    queryVolumeList() {
      this.resData.loading = true;
      const variables = {
        keyword: "",
        userID: this.userID,
        num: 10000,
      };
      this.apollo
        .query(this.url.volume, baseGql.queryVolumeList, variables)
        .then((res) => {
          this.resData.loading = false;
          if (res.data) {
            let finalVolumeInfo = [];
            if (this.searchVal && this.searchVal.trim() !== "") {
              res.data.listVolume.forEach((user) => {
                if (user.name.indexOf(this.searchVal.trim()) > -1) {
                  finalVolumeInfo.push(user);
                }
              });
            } else {
              finalVolumeInfo = res.data.listVolume;
            }
            // 按创建时间倒序排列
            finalVolumeInfo.sort(function (a, b) {
              return b.createTime - a.createTime;
            });
            this.volumeList = finalVolumeInfo;
            this.resData.page.totalRecord = res.data.listVolume.length;
            this.handleCurrentChange(1);
            this.totalCapacity = 0;
            this.volumeList.forEach((item) => {
              this.totalCapacity += item.capacity;
              item.occupiedPercent =
                (
                  (item.occupied / 1024 / 1024 / 1024 / item.capacity) *
                  100
                ).toFixed(2) + "%";
              item.capacityStr = item.capacity + "G";
              item.occupiedStr =
                (item.occupied / 1024 / 1024 / 1024).toFixed(4) + "G";
              this.accessKey = item.oSSAccessKey;
              this.secretKey = item.oSSSecretKey;
              if (item.createTime) {
                item.createTime = item.createTime * 1000;
              }
              item.createTime =
                date2Str(item.createTime, "-") +
                " " +
                time2Str(item.createTime, ":");
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
    createVolume(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const variables = {
            authenticate: false,
            capacity: parseInt(this.createVolumeForm.totalCapacity),
            crossZone: false,
            dataPartitionSize: 0,
            dpReplicaNum: 3,
            enableToken: false,
            followerRead: false,
            mpCount: 0,
            name: this.createVolumeForm.name,
            owner: this.createVolumeForm.owner,
            zoneName: this.createVolumeForm.zoneName,
            description: this.createVolumeForm.notes,
          };
          this.apollo
            .mutation(this.url.volume, baseGql.createVolume, variables)
            .then((res) => {
              if (res.code === 200) {
                this.createVolumeDialog = false;
                this.queryVolumeList();
                this.$message({
                  message: this.$t("chubaoFS.message.Success"),
                  type: "success",
                });
              } else {
                this.$message.error(res.message);
              }
            });
        } else {
          return false;
        }
      });
    },
    updateVolume() {
      const variables = {
        authKey: this.md5(this.editVolumeForm.owner),
        name: this.editVolumeForm.name,
        replicaNum: parseInt(this.editVolumeForm.dpReplicaNum),
        description: this.desContent,
      };
      if (variables.replicaNum > this.editRow.dpReplicaNum) {
        this.$message.error(
          this.$t("chubaoFS.operations.VolumeManagement.updateVolumeError")
        );
        return;
      }
      this.apollo
        .mutation(this.url.volume, baseGql.updateVolume, variables)
        .then((res) => {
          if (res.code === 200) {
            this.queryVolumeList();
            this.editVolumeDialog = false;
            this.$message({
              message: this.$t("chubaoFS.message.Success"),
              type: "success",
            });
          } else {
            this.$message.error(res.message);
          }
        });
    },
    extendVolume() {
      const variables = {
        authKey: this.md5(this.editVolumeForm.owner),
        name: this.editVolumeForm.name,
        capacity: parseInt(this.extendCapacity),
      };
      if (variables.capacity < this.editVolumeForm.capacity) {
        const extendVolumeError = this.$t(
          "chubaoFS.operations.VolumeManagement.extendVolumeError"
        );
        extendVolumeError
          .replace("capacityVal", variables.capacity)
          .replace("oldCapacityVal", this.editVolumeForm.capacity);
        this.$message.error(extendVolumeError);
        return;
      }
      this.apollo
        .mutation(this.url.volume, baseGql.updateVolume, variables)
        .then((res) => {
          if (res.code === 200) {
            this.queryVolumeList();
            this.extendCapacity = null;
            this.volumeExtensionDialog = false;
            this.$message({
              message: this.$t("chubaoFS.message.Success"),
              type: "success",
            });
          } else {
            this.$message.error(res.message);
          }
        });
    },
    deleteVolume(row) {
      const variables = {
        authKey: this.md5(row.owner),
        name: row.name,
      };
      this.$confirm(
        this.$t("chubaoFS.operations.VolumeManagement.deleteVolTip") +
          " (" +
          row.name +
          ")",
        this.$t("chubaoFS.tools.Warning"),
        {
          confirmButtonText: this.$t("chubaoFS.tools.Yes"),
          cancelButtonText: this.$t("chubaoFS.tools.No"),
          type: "warning",
        }
      )
        .then(() => {
          this.apollo
            .mutation(this.url.volume, baseGql.deleteVolume, variables)
            .then((res) => {
              if (res.code === 200) {
                this.queryVolumeList();
                this.$message({
                  message: this.$t("chubaoFS.message.Success"),
                  type: "success",
                });
              } else {
                this.$message.error(res.message);
              }
            });
        })
        .catch(() => {});
    },
    queryPermissionList() {
      this.resData.loading = true;
      const variables = {
        volName: this.permissionItem.name,
        userID: this.permissionItem.owner,
      };
      this.apollo
        .query(this.url.volume, baseGql.queryPermissionList, variables)
        .then((res) => {
          this.resData.loading = false;
          if (res) {
            this.permissionList = res.data.volPermission;
          }
        })
        .catch((error) => {
          this.resData.loading = false;
          this.$Message.error(error);
        });
    },
    grantPermission(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const variables = {
            policy: [this.grantPermissionForm.access],
            userID: this.grantPermissionForm.userName,
            volume: this.permissionItem.name,
          };
          this.apollo
            .mutation(this.url.user, baseGql.grantPermission, variables)
            .then((res) => {
              if (res.code === 200) {
                this.grantPermissionDialog = false;
                this.queryPermissionList();
                this.$message({
                  message: this.$t("chubaoFS.message.Success"),
                  type: "success",
                });
              } else {
                this.$message.error(res.message);
              }
            });
        } else {
          return false;
        }
      });
    },
    editPermission() {
      const variables = {
        policy: [this.editPermissionForm.access],
        userID: this.editPermissionForm.userID,
        volume: this.permissionItem.name,
      };
      this.apollo
        .mutation(this.url.user, baseGql.grantPermission, variables)
        .then((res) => {
          if (res.code === 200) {
            this.editPermissionDialog = false;
            this.queryPermissionList();
            this.$message({
              message: this.$t("chubaoFS.message.Success"),
              type: "success",
            });
          } else {
            this.$message.error(res.message);
          }
        });
    },
    deletePermission(row) {
      const variables = {
        userID: row.userID,
        volume: this.permissionItem.name,
      };
      this.$confirm(
        this.$t("chubaoFS.operations.VolumeManagement.deletePermTip"),
        this.$t("chubaoFS.tools.Warning"),
        {
          confirmButtonText: this.$t("chubaoFS.tools.Yes"),
          cancelButtonText: this.$t("chubaoFS.tools.No"),
          type: "warning",
        }
      )
        .then(() => {
          this.apollo
            .mutation(this.url.user, baseGql.deletePermission, variables)
            .then((res) => {
              if (res.code === 200) {
                this.queryPermissionList();
                this.$message({
                  message: this.$t("chubaoFS.message.Success"),
                  type: "success",
                });
              } else {
                this.$message.error(res.message);
              }
            });
        })
        .catch(() => {});
    },
    openGrantPermission() {
      this.grantPermissionDialog = true;
    },
    openEditPermission(row) {
      this.editPermissionDialog = true;
      this.editPermissionForm = Object.assign({}, row);
      this.editPermissionForm.access = this.editPermissionForm.access.join(",");
    },
    openDialog(tag, row) {
      if (tag === "createVolume") {
        this.createVolumeDialog = true;
      }
      if (tag === "edit") {
        this.editVolumeDialog = true;
        this.editVolumeForm = Object.assign({}, row);
        this.desContent = this.editVolumeForm.toSimpleVolView.description;
        this.editRow = row;
      }
      if (tag === "notes") {
        this.notesDialog = true;
        this.editVolumeForm = Object.assign({}, row);
        this.description = this.editVolumeForm.toSimpleVolView.description;
      }
      if (tag === "extend") {
        this.volumeExtensionDialog = true;
        this.editVolumeForm = Object.assign({}, row);
      }
      if (tag === "permission") {
        this.permissionDialog = true;
        this.permissionItem = row;
        this.queryPermissionList(row);
      }
    },
    closeDialog(tag) {
      if (tag === "create") {
        this.createVolumeDialog = false;
        this.createVolumeForm = {};
        this.$refs["createVolumeForm"].resetFields();
      }
      if (tag === "extend") {
        this.volumeExtensionDialog = false;
        this.extendCapacity = null;
      }
      if (tag === "grantPer") {
        this.grantPermissionDialog = false;
        this.grantPermissionForm = {};
        this.$refs["grantPermissionForm"].resetFields();
      }
      if (tag === "editPer") {
        this.editPermissionDialog = false;
      }
    },
    checkNumber(rule, value, callback) {
      if (!/(^[1-9]\d*$)/.test(value)) {
        return callback(
          new Error(this.$t("chubaoFS.enterRules.checkNumberTip"))
        );
      } else {
        callback();
      }
    },
    checkVolName(rule, value, callback) {
      if (!/(^[a-z0-9]{1}[a-z0-9\\-]{1,61}[a-z0-9]$)/.test(value)) {
        return callback(
          new Error(
            this.$t("chubaoFS.operations.VolumeManagement.checkVolNameTip")
          )
        );
      } else {
        callback();
      }
    },
    checkOwnerId(rule, value, callback) {
      if (!/(^[A-Za-z][A-Za-z0-9_]{0,20}$)/.test(value)) {
        return callback(
          new Error(
            this.$t("chubaoFS.operations.VolumeManagement.checkOwnerIdTip")
          )
        );
      } else {
        callback();
      }
    },
  },
  mounted() {
    // this.queryVolumeList()
  },
};
</script>

<style scoped>
.volume-left {
  display: inline-block;
}
.volume-right {
  display: inline-block;
  float: right;
}
.volume-right span {
  margin-right: 15px;
}
.volume-summary {
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
