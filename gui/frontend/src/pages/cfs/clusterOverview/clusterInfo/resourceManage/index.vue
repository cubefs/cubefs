<template>
  <div>
    <el-radio-group v-model="activeName" style="margin-bottom: 10px;">
      <el-radio-button label="node">多副本节点</el-radio-button>
      <el-radio-button label="blobStoreNode" :disabled="!ebsClusterList || !ebsClusterList.length">纠删码节点</el-radio-button>
      <el-radio-button label="metaNode">元数据节点</el-radio-button>
    </el-radio-group>
    <div class="mg-bt-s flex" v-if="activeName !== 'blobStoreNode'">
      <span class="fontType"><span>总节点数:</span> <span class="mg-lf-m"></span>{{ info.node }}</span>
      <span class="fontType mg-lf-m"><span>总分区数:</span> <span class="mg-lf-m"></span>{{ info.partition }}</span>
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
        </el-progress></div>

    </div>
    <component
      :is="items[activeName].component"
      v-if="items[activeName].name === activeName"
      :info.sync="info"
    />

  </div>
</template>
<script>
import Node from './dataNode/node.vue'
import MetaNode from './metaNode/metaNode.vue'
import BlobStoreNode from './blobStoreNode/index.vue'
import { renderSize } from '@/utils'
import mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  name: '',
  components: { Node, MetaNode, BlobStoreNode },
  mixins: [mixin],
  filters: {
    renderSize(val) {
      const data = renderSize(val, 1)
      return data
    },
  },
  props: [''],
  data () {
    return {
      activeName: 'node',
      items: {
        node: {
          name: 'node',
          component: 'Node',
        },
        metaNode: {
          name: 'metaNode',
          component: 'MetaNode',
        },
        blobStoreNode: {
          name: 'blobStoreNode',
          component: 'BlobStoreNode',
        }
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
</style>
