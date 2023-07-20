<template>
  <div>
    <div class="fontTypeSpan">
      <span
        class="mg-lf-m"
      ><span>vid:</span><span class="mg-lf-m">{{ curVol.vid }}</span></span>
      <span
        class="mg-lf-m"
      ><span>条带组状态:</span><span class="mg-lf-m">{{ statusMap[curVol.status] }}</span></span>
      <span
        class="mg-lf-m"
      ><span>健康度:</span><span class="mg-lf-m">{{ curVol.health_score === 0 ? '没有不可写的磁盘' : `有${Math.abs(curVol.health_score)}个磁盘不可写` }}</span></span>
      <span
        class="mg-lf-m"
      ><span>code_mode:</span><span class="mg-lf-m">{{ codeMap[curVol.code_mode] }}</span></span>
      <span
        class="mg-lf-m"
      ><span>总空间:</span><span class="mg-lf-m">{{ curVol.total | readablizeBytes }}</span></span>
      <span
        class="mg-lf-m"
      ><span>已使用:</span><span class="mg-lf-m">{{ curVol.used| readablizeBytes }}</span></span>
      <span
        class="mg-lf-m"
      ><span>空闲:</span><span class="mg-lf-m">{{ curVol.free | readablizeBytes }}</span></span>
    </div>
    <el-tabs v-model="activeName" class="inside" @tab-click="handleClick">
      <el-tab-pane
        v-for="item in tabs"
        :key="item.name"
        :label="item.label"
        :name="item.name"
      >
        <component
          :is="item.component"
          v-if="item.name === activeName"
          :disk-arr="diskArr"
          @curVolChange="detailUpdate"
          @activeChange="activeChange"
          @diskArrChange="diskArrChange"
        />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
<script>
import DataBlock from './detail.vue'
import DiskList from './nodeDetail.vue'
import { readablizeBytes, codeMap } from '@/utils'
export default {
  name: '',
  components: { DataBlock, DiskList },
  filters: {
    readablizeBytes(value) {
      return readablizeBytes(value)
    },
  },
  data () {
    return {
      curVol: {},
      activeName: 'dataBlock',
      diskArr: [],
      statusMap: {
        1: 'idle',
        2: 'active',
        3: 'lock',
        4: 'unlocking',
      },
      codeMap,
    }
  },
  computed: {
    tabs() {
      return [
        {
          label: '数据块',
          name: 'dataBlock',
          component: 'DataBlock',
        },
        {
          label: '磁盘列表',
          name: 'diskList',
          component: 'DiskList',
        },
      ]
    },
  },
  watch: {
  },
  created() {
  },
  beforeMount() {},
  mounted() {
  },
  methods: {
    activeChange(name) {
      this.activeName = name
    },
    goDetail(row) {
      this.$router.replace({ query: { vid: row.vid } })
      this.drawer = true
      // this.curVol = row
    },
    detailUpdate(val) {
      this.curVol = val
    },
    diskArrChange(val) {
      this.diskArr = val
    },
    handleClick() {
      this.$router.replace({ query: { vid: this.$route.query.vid } })
    },
  },
}
</script>
<style lang='scss' scoped>

.fontTypeSpan {
font-family: 'OPPOSans M';
font-style: normal;
font-weight: 400;
font-size: 16px;
line-height: 20px;
}
.mg-lf-l{
  margin-left:10px;
}
.inside{
  margin: 10px;
}
::v-deep .el-drawer__body {
  overflow: auto;
}
/*2.隐藏滚动条，太丑了*/
::v-deep .el-drawer__container ::-webkit-scrollbar{
    width: 10px;
}
</style>
