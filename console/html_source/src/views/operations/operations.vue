<template>
  <div class="operations">
    <el-tabs v-model="operationsSelect" type="card" @tab-click="handleClick">
      <el-tab-pane :label="$t('chubaoFS.operations.ServerManagement.title')" name="ServerManagement">
        <div class="operation-container"><server-management ref="childServer"></server-management></div>
      </el-tab-pane>
      <el-tab-pane :label="$t('chubaoFS.operations.VolumeManagement.title')" name="VolumeManagement">
        <div class="operation-container"> <volume-management ref="childVolume"></volume-management></div>
      </el-tab-pane>
      <el-tab-pane :label="$t('chubaoFS.operations.MetaPartitionManagement.title')" name="MetaPartitionManagement">
        <div class="operation-container"> <metaPartition-management ref="childMetaPartition"></metaPartition-management></div>
      </el-tab-pane>
      <el-tab-pane :label="$t('chubaoFS.operations.DataPartitionManagement.title')" name="DataPartitionManagement">
        <div class="operation-container"> <dataPartition-management ref="childDataPartition"></dataPartition-management></div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
import ServerManagement from './serverManagement'
import VolumeManagement from './volumeManagement'
import MetaPartitionManagement from './metaPartitionManagement'
import DataPartitionManagement from './dataPartitionManagement'
export default {
  name: 'Operations',
  components: {
    ServerManagement,
    VolumeManagement,
    MetaPartitionManagement,
    DataPartitionManagement,
  },
  data () {
    return {
      operationsSelect: 'ServerManagement'
    }
  },
  methods: {
    handleClick (tab) {
      if (tab.name === 'ServerManagement') {
        this.$refs.childServer.queryServerList()
      }
      if (tab.name === 'VolumeManagement') {
        this.$refs.childVolume.queryVolumeList()
      }
      if (tab.name === 'MetaPartitionManagement') {
        this.$refs.childMetaPartition.queryMetaPartitionList()
      }
      if (tab.name === 'DataPartitionManagement') {
        this.$refs.childDataPartition.queryDataPartitionList()
      }
    }
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
