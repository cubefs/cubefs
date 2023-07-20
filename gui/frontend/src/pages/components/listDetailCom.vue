<template>
  <el-dialog
    title="详情"
    :visible.sync="dialogVisible"
    width="50%"
    :before-close="handleClose"
  >
    <el-card v-if="applyInfo" shadow="hover">
      <div class="detail_title">申请信息</div>
      <ul class="more_ul">
        <li v-for="i in applyInfo" :key="i.key">
          <div
            v-if="i.key === 'id' || i.key === 'purpose' || i.type === 'full'"
            class="li_div"
          >
            <div class="width20">{{ i.name }}</div>
            <div
              class="width80"
            >{{ i.renderFun ? i.renderFun(allData[i.key], allData) : allData[i.key] }}</div>
          </div>
        </li>
      </ul>
      <ul class="more_ul">
        <li v-for="i in applyInfo" :key="i.key">
          <div
            v-if="i.key !== 'id' && i.key !== 'purpose' && i.type !== 'full'"
            class="li_div width50"
          >
            <div class="width40">{{ i.name }}</div>
            <div
              class="width60"
            >{{ i.renderFun ? i.renderFun(allData[i.key], allData) : allData[i.key] }}</div>
          </div>
        </li>
      </ul>
    </el-card>
    <el-card v-if="approverInfo" shadow="hover">
      <div class="detail_title">审核信息</div>
      <div class="more_ul">
        <el-row
          v-for="i in approverInfo"
          :key="i.key"
          class="more_row"
          :gutter="20"
        >
          <el-col :span="6">{{ i.name }}</el-col>
          <el-col :span="18">
            <div
              v-if="i.name==='审批结果'"
            >{{ i.renderFun(allData[i.key]) + (allData['remark'] ? `(${allData['remark']})` : '') }}</div>
            <div
              v-else
            >{{ i.renderFun ? i.renderFun(allData[i.key]) : allData[i.key] }}</div>
          </el-col>
        </el-row>
      </div>
    </el-card>
    <el-card v-if="regionInfo" shadow="hover">
      <div class="detail_title">区域信息</div>
      <div v-for="(i, index) in regionInfo" :key="index">
        <el-row
          v-if="i.type!=='endpoint' && i.key"
          class="more_row"
          :gutter="20"
        >
          <el-col :span="6">{{ i.name }}</el-col>
          <el-col :span="18">{{ i.key }}</el-col>
        </el-row>
        <el-row
          v-if="i.type==='endpoint' && i.key"
          class="more_row"
          :gutter="20"
        >
          <el-col :span="6">{{ `Endpoint(${i.name})` }}</el-col>
          <el-col :span="10">{{ i.key }}</el-col>
          <el-col v-if="i.remark" :span="2">备注</el-col>
          <el-col :span="4">{{ i.remark }}</el-col>
        </el-row>
      </div>
    </el-card>
    <el-card v-if="otherInfo" shadow="hover">
      <div class="detail_title">其他信息</div>
      <el-row v-for="i in otherInfo" :key="i.key" class="more_row" :gutter="20">
        <el-col :span="8">{{ i.name }}</el-col>
        <el-col
          :span="16"
        >{{ i.renderFun ? i.renderFun(allData[i.key]) : allData[i.key] }}</el-col>
      </el-row>
    </el-card>
  </el-dialog>
</template>

<script>
export default {
  data() {
    return {
      dialogVisible: false,
      applyInfo: [],
      regionInfo: [],
      approverInfo: [],
      otherInfo: [],
      allData: {},
    }
  },
  methods: {
    init(row, keys) {
      this.allData = row
      const { applyInfo, approverInfo, otherInfo, regionInfo } = keys
      this.applyInfo = applyInfo
      this.approverInfo = approverInfo
      this.regionInfo = regionInfo
      this.otherInfo = otherInfo

      this.dialogVisible = true
    },
    handleClose() {
      this.dialogVisible = false
      this.applyInfo = []
      this.approverInfo = []
      this.regionInfo = []
      this.otherInfo = []
      this.allData = {}
    },
  },
}
</script>

<style lang="scss" scoped>
.detail_title {
  height: 20px;
  line-height: 20px;
  border-left: 4px solid #66cc99;
  padding-left: 8px;
  font-size: 14px;
  margin-bottom: 12px;
}
.more_ul {
  padding: 0 8px;
  overflow: hidden;
  .li_div {
    overflow: hidden;
    margin: 6px 0;
  }
}
.more_row {
  margin: 8px 0;
}
.width50 {
  float: left;
  width: 50%;
}
.width20 {
  float: left;
  width: 20%;
}
.width80 {
  float: left;
  width: 80%;
}
.width40 {
  float: left;
  width: 40%;
}
.width60 {
  float: left;
  width: 60%;
}
</style>
