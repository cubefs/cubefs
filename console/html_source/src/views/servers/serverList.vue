<template>
  <div class="operations">
    <crumb :crumbInfo="$t('chubaoFS.servers.ServerList')"></crumb>
    <el-tabs v-model="operationsSelect" type="card">
      <el-tab-pane :label="$t('chubaoFS.servers.Master')" name="Master">
        <div class="operation-container"><master></master></div>
      </el-tab-pane>
      <el-tab-pane :label="$t('chubaoFS.servers.Matanode')" name="Matanode">
        <div class="operation-container"><metanode></metanode></div>
      </el-tab-pane>
      <el-tab-pane :label="$t('chubaoFS.servers.Datanode')" name="Datanode">
        <div class="operation-container"> <datanode></datanode></div>
      </el-tab-pane>
      <!--<el-tab-pane label="Fuse Clients" name="Fuse Clients">-->
        <!--<div class="operation-container"><fuse-clients></fuse-clients></div>-->
      <!--</el-tab-pane>-->
    </el-tabs>
  </div>
</template>

<script>
import Master from './master'
import Metanode from './metanode'
import Datanode from './datanode'
import FuseClients from './fuseClients'
export default {
  name: 'ServerLists',
  components: {
    Master,
    Metanode,
    Datanode,
    FuseClients
  },
  data () {
    return {
      operationsSelect: 'Master'
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
