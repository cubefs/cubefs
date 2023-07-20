<template>
  <div class="card">
    <div class="card_title m-b-5">副本{{ indexMap[index] }} <span v-if="replica.IsLeader">(主)</span></div>
    <el-row
      style="padding: 5px 0px"
    >
      <el-col :span="3"><span class="key_title">files:</span></el-col>
      <el-col :span="9"><span class="value_title">{{ replica.FileCount }}</span></el-col>
      <el-col :span="3"><span class="key_title">addr:</span></el-col>
      <el-col :span="9"><span class="value_title">{{ replica.Addr }}</span></el-col>
    </el-row>
    <el-row
      style="padding: 5px 0px"
    >
      <el-col :span="3"><span class="key_title">Leader:</span></el-col>
      <el-col :span="9"><span class="value_title">{{ replica.IsLeader }}</span></el-col>
      <el-col :span="3"><span class="key_title">disk:</span></el-col>
      <el-col :span="9"><span class="value_title">{{ replica.DiskPath }}</span></el-col>
    </el-row>
    <el-row
      style="padding: 5px 0px"
    >
      <el-col :span="3"><span class="key_title">status:</span></el-col>
      <el-col :span="9"><span class="value_title">{{ replica.Status | filterStatus }}</span></el-col>
      <el-col :span="3"><span class="key_title">report:</span></el-col>
      <el-col :span="9"><span class="value_title">{{ (replica.ReportTime * 1000) | formatDate }}</span></el-col>
    </el-row>
    <el-row
      style="padding: 5px 0px"
    >
      <el-col :span="3"><span class="key_title">hostID:</span></el-col>
      <el-col :span="9"><span class="value_title">{{ host }}</span></el-col>
      <el-col :span="3"><span class="key_title">zone:</span></el-col>
      <el-col :span="9"><span class="value_title">{{ zone }}</span></el-col>
    </el-row>
    <el-progress class="m-t-15" :stroke-width="15" :percentage="Math.floor(replica.UsedSize*100/replica.TotalSize)" :format="format"></el-progress>
    <div style="display: flex; justify-content: space-between; margin-top: 5px;">
      <div>
        <span class="key_title m-r-5">used:</span>
        <span :class="{ 'used-error': usedError }">{{ replica.UsedSize | renderSize }} <i v-if="usedError" class="el-icon-warning-outline"></i></span>
      </div>
      <div>
        <span class="key_title m-r-5">total:</span>
        <span>{{ replica.TotalSize | renderSize }}</span>
      </div>
    </div>
  </div>
</template>

<script>
import { renderSize, formatDate } from '@/utils'
export default {
  filters: {
    renderSize(val) {
      return renderSize(val)
    },
    formatDate(val) {
      return formatDate(val)
    },
    filterStatus(val) {
      switch (val) {
        case 1:
          return 'ReadOnly'
        case 2:
          return 'ReadWrite'
        case -1:
          return 'Unavailable'
        default:
          return 'Unavailable'
      }
    },
  },
  props: {
    replica: {
      type: Object,
      default: () => {},
    },
    zone: {
      type: String,
      default: '',
    },
    host: {
      type: String,
      default: '',
    },
    index: {
      type: Number,
      default: 0,
    },
    usedError: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      indexMap: ['一', '二', '三'],
    }
  },
  methods: {
    format() {
      return ''
    },
  },
}

</script>
<style scoped lang='scss'>
.card {
  padding: 16px;
  width: 518px;
  height: 200px;
  border-radius: 4px;
  background: #F8F9FC;
}
.card_title {
  font-size: 16px;
  font-weight: bold;
  color: #000;
}
.key_title {
  color: rgba(0, 0, 0, 0.6);
}
.value_title {
  color: rgba(0, 0, 0, 0.8);
}
::v-deep {
  .el-progress-bar {
    padding: 0;
  }
}
.used-error {
  color: #D54941;
}
</style>
