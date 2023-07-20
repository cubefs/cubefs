<template>
  <div class="card">
    <div class="card_title m-b-5">副本{{ indexMap[index] }} <span v-if="index === 0">(主)</span></div>
    <el-row
      style="padding: 5px 0px"
    >
      <el-col :span="5"><div class="key_title">applyId:</div></el-col>
      <el-col :span="7"><div class="value_title">{{ replica.ApplyID }}</div></el-col>
      <el-col :span="5"><div class="key_title">addr:</div></el-col>
      <el-col :span="7"><div class="value_title" :title="replica.Addr">{{ replica.Addr }}</div></el-col>
    </el-row>
    <el-row
      style="padding: 5px 0px"
    >
      <el-col :span="5"><div class="key_title">inodeCount:</div></el-col>
      <el-col :span="7"><div class="value_title">{{ replica.InodeCount }}</div></el-col>
      <el-col :span="5"><div class="key_title">maxInode:</div></el-col>
      <el-col :span="7"><div class="value_title">{{ replica.MaxInode }}</div></el-col>
    </el-row>
    <el-row
      style="padding: 5px 0px"
    >
      <el-col :span="5"><div class="key_title">状态:</div></el-col>
      <el-col :span="7"><div class="value_title">{{ replica.Status }}</div></el-col>
      <el-col :span="5"><div class="key_title">目录数:</div></el-col>
      <el-col :span="7"><div class="value_title">{{ replica.DentryCount }}</div></el-col>
    </el-row>
    <el-row
      style="padding: 5px 0px"
    >
      <el-col :span="5"><div class="key_title">hostID:</div></el-col>
      <el-col :span="7"><div class="value_title" :title="host">{{ host }}</div></el-col>
      <el-col :span="5"><div class="key_title">zone:</div></el-col>
      <el-col :span="7"><div class="value_title">{{ zone }}</div></el-col>
    </el-row>
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
  height: 150px;
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
  overflow:hidden;
  text-overflow:ellipsis;
  white-space:nowrap;
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
