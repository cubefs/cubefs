<template>
  <div>
    <div class="mg-bt-s">
      <div>
        <el-radio-group v-model="activeName" style="margin-bottom: 10px;">
          <el-radio-button label="partition">多副本</el-radio-button>
          <el-radio-button label="blobstoreVolume" :disabled="!ebsClusterList || !ebsClusterList.length">纠删码</el-radio-button>
        </el-radio-group>
      </div>
      <div v-if="activeName === 'partition'" class="flex">
        <span class="fontType"><span>总节点数:</span> <span class="mg-lf-m"></span>{{ info.node }}</span>
        <span class="fontType mg-lf-m"><span>总分区数:</span> <span class="mg-lf-m"></span>{{ info.partition }}</span>
        <span class="fontType mg-lf-m"><span>损坏分区数:</span> <span class="mg-lf-m"></span><span class="bad_partition" @click="showDialog('DP状态')">{{ badDataPartitionNum }}</span>/{{ (badDataPartitionNum / info.partition * 100).toFixed()+'%' || '0%' }}</span>
        <span class="fontType mg-lf-m"><span>总容量:</span> <span class="mg-lf-m"></span>{{ info.total |renderSize }}</span>
        <div class="mg-lf-m progress">
          <span>{{ info.used |renderSize }}/{{ (info.used/info.total*100).toFixed(0)+'%' }}</span>
          <el-progress
            v-if="info.node!==0"
            :stroke-width="10"
            :show-text="false"
            :percentage="info.used/info.total*100"
            :color="[
              { color: '#f56c6c', percentage: 100 },
              { color: '#e6a23c', percentage: 80 },
              { color: '#5cb87a', percentage: 60 },
              { color: '#1989fa', percentage: 40 },
              { color: '#6f7ad3', percentage: 20 },
            ]"
          >
          </el-progress>
        </div>
      </div>
    </div>
    <component
      :is="items[activeName].component"
      v-if="items[activeName].name === activeName"
      :info.sync="info"
    />
    <el-dialog
      v-if="DataPartitionDetailDialogVisible"
      title="坏DP详情"
      width="65%"
      :visible.sync="DataPartitionDetailDialogVisible"
      center
    >
      <el-table
        :data="PartitionTableData"
        style="width: 100%"
      >
        <el-table-column
          label="分区ID"
          prop="PartitionID"
          :width="100"
        ></el-table-column>
        <el-table-column label="卷名" prop="VolName"></el-table-column>
        <el-table-column
          label="副本数"
          prop="ReplicaNum"
        ></el-table-column>
        <el-table-column label="isRecovering" :width="100">
          <template slot-scope="scope">
            <span>{{ scope.row.IsRecover }}</span>
          </template>
        </el-table-column>
        <el-table-column label="Leader" prop="Leader" width="180"></el-table-column>
        <el-table-column label="Members" width="180">
          <template slot-scope="scope">
            <div v-for="item in scope.row.Members" :key="item">{{ item }}</div>
          </template>
        </el-table-column>
        <el-table-column
          label="状态"
          prop="Status"
          :width="150"
        ></el-table-column>
      </el-table>
    </el-dialog>
    <el-dialog
      v-if="DataPartitionDialogVisible"
      title="坏DP"
      width="65%"
      :visible.sync="DataPartitionDialogVisible"
      center
      top="5vh"
    >
      <div>缺少副本的分区</div>
      <el-table
        max-height="350"
        :data="LackReplicaDataPartitionIDs"
        style="width: 100%"
      >
        <el-table-column
          label="序号"
          type="index"
        >
        </el-table-column>
        <el-table-column
          label="分区ID"
          prop="id"
        >
          <template slot-scope="scope">
            <div>{{ scope.row }}</div>
          </template></el-table-column>
        <el-table-column
          label="副本数"
          prop="ReplicaNum"
        ></el-table-column>
        <el-table-column
          label="操作"
        >
          <template slot-scope="scope">
            <el-button
              size="medium"
              type="text"
              @click="showDetail(scope.row, 2)"
            >详情</el-button>
          </template>
          ></el-table-column>
      </el-table>
      <div>缺少leader的分区</div>
      <el-table
        max-height="300"
        :data="CorruptDataPartitionIDs"
        style="margin-top:5px"
      >
        <el-table-column
          label="序号"
          type="index"
        >
        </el-table-column>
        <el-table-column
          label="分区ID"
          prop="id"
        >
          <template slot-scope="scope">
            <div>{{ scope.row }}</div>
          </template></el-table-column>
        <el-table-column
          label="副本数"
          prop="ReplicaNum"
        ></el-table-column>
        <el-table-column
          label="操作"
        >
          <template slot-scope="scope">
            <el-button
              size="medium"
              type="text"
              @click="showDetail(scope.row, 2)"
            >详情</el-button>
          </template>
          ></el-table-column>
      </el-table>
    </el-dialog>
  </div>
</template>
<script>
import Partition from './partition.vue'
import BlobstoreVolume from './blobStoreVolume'
import { renderSize } from '@/utils'
import mixin from '@/pages/cfs/clusterOverview/mixin'
import graphics from '../components/graphics'
export default {
  name: '',
  components: { Partition, BlobstoreVolume },
  filters: {
    renderSize(val) {
      const data = renderSize(val, 1)
      return data
    },
  },
  mixins: [mixin, graphics],
  props: [''],
  data () {
    return {
      activeName: 'partition',
      items: {
        partition: {
          name: 'partition',
          component: 'Partition',
        },
        blobstoreVolume: {
          name: 'blobstoreVolume',
          component: 'BlobstoreVolume',
        },
      },
      info: {
        node: 0,
        partition: 0,
        total: 0,
        used: 0,
      },
    }
  },
  computed: {},
  watch: {},
  created() {},
  beforeMount() {},
  mounted() {},
  methods: {},
}
</script>
<style lang='scss' scoped>
.fontType{
font-family: 'Microsoft YaHei';
font-style: normal;
font-weight: 400;
font-size: 14px;
line-height: 14px;
/* identical to box height, or 167% */
color: #000000;
}
.progress{
  width: 100px;
  position: relative;
  top: -5px;
  left: 10px;
}
.bad_partition {
  cursor: pointer;
  color: #38c59f;
}
</style>
