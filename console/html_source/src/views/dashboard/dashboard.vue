<template>
  <div class="operations">
    <el-tabs v-model="operationsSelect" type="card">
      <el-tab-pane label="Clusters" name="Clusters" lazy>
        <div class="operation-container"><clusters></clusters></div>
      </el-tab-pane>
      <el-tab-pane label="Volume" name="Volume" lazy>
        <div class="operation-container"><volume></volume></div>
      </el-tab-pane>
      <el-tab-pane label="Master" name="Master" lazy>
        <div class="operation-container"><master></master></div>
      </el-tab-pane>
      <el-tab-pane label="MetaNode" name="MetaNode" lazy>
        <div class="operation-container"> <meta-nodes></meta-nodes></div>
      </el-tab-pane>
      <el-tab-pane label="DataNode" name="DataNode" lazy>
        <div class="operation-container"><data-nodes></data-nodes></div>
      </el-tab-pane>
      <el-tab-pane label="FuseClient" name="FuseClient" lazy>
        <div class="operation-container"><fuse-clients></fuse-clients></div>
      </el-tab-pane>
      <el-tab-pane label="ObjectNode" name="ObjectNode" lazy>
        <div class="operation-container"><object-nodes></object-nodes></div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
import Clusters from './clusters'
import MetaNodes from './metanodes'
import DataNodes from './datanodes'
import ObjectNodes from './objectNode'
import FuseClients from './fuseClients'
import Master from './master'
import GoRuntime from './goRuntime'
import Volume from './volume'
export default {
  name: 'dashboard',
  components: {
    Clusters,
    MetaNodes,
    DataNodes,
    ObjectNodes,
    FuseClients,
    Master,
    GoRuntime,
    Volume
  },
  data () {
    return {
      operationsSelect: 'Clusters'
    }
  },
  methods: {
  }
}
</script>

<style>
  .operation-container{
    width:100%;
    min-height: calc(100vh - 180px);
    padding: 20px;
    box-sizing:border-box;
    background: #fff;
  }
  .operations .el-tabs__nav-wrap{
    margin-bottom: 0;
  }
  .operations .el-tabs__header{
    margin-bottom: 0;
  }
  .operations .el-tabs--card>.el-tabs__header .el-tabs__nav{
    border:none;
  }
  .operations .el-tabs--card>.el-tabs__header{
    border:none;
  }
  .operations .el-tabs--card>.el-tabs__header .el-tabs__item{
    border:none;
  }
  .operations .el-tabs__item{
    background: #E3E8ED;
    color:#333333;
    border:none;
    margin-right: 10px;
    border-top-left-radius: 5px;
    border-top-right-radius: 5px;
  }
  .operations .el-tabs--card>.el-tabs__header .el-tabs__item.is-active{
    background: #fff;
    color:#06268D;
  }
</style>
