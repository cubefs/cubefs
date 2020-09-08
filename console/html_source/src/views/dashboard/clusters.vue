<template>
  <div class="dash-cluster">
    <!--<h2>Cluster：Baud Storage </h2>-->
    <div class="total-title mt10 mb30">
      <ul class="total-ul">
        <li v-for="item in timeInterval" :key="item" :class="{'active':  item=== totalTimeInterval}" @click="changeTime(item)">{{item}}</li>
      </ul>
      <span>TopK:</span>
      <topk-sel class="inline-block ml10"></topk-sel>
    </div>
    <!--COUNT-->
    <div class="mt20">
      <div class="clearfix">
        <div class="count fl" v-for="(item, index) in countList" :key="index">
          <span>{{item.text}}</span>
          <i class="el-icon-loading cont-loading" v-show="item.isLoading"></i>
          <b v-if="item.id != 'VolumeCount'">{{item.num}}</b>
          <b v-else>{{item.num}} <em class="sub-text">days before</em></b>
          <div :id="item.id" style="width:100%; height: 60px;" class="cluster_pic"></div>
        </div>
      </div>
    </div>
    <!--SIZE-->
    <div class="mt20 ratio-block clearfix">
      <!--DataNodeSize-->
      <div class="each-block b-small-size fl">
        <h3 class="mb15">DataNodeSize</h3>
        <div v-for="(item, index) in dataNodeSize" :key="index" class="mb10">
          <h4 class="mb5">{{item.name}}</h4>
          <div class="size-block">
            <div v-if="item.size > 0" class="size-bar" :style="{'width':(item.size / dataNodeSize[0].size * 100)+ '%'}">
              <span class="size-val">{{item.unitSize}}</span>
            </div>
            <div v-else-if="item.size === 0 || item.size < 0">
              <span class="size-val">{{item.size}} GB</span>
            </div>
            <div v-else>
              <span class="size-val">-</span>
            </div>
          </div>
        </div>
      </div>
      <!--MetaNodeSize-->
      <div class="each-block b-small-size fl">
        <h3 class="mb15">MetaNodeSize</h3>
        <div v-for="(item, index) in metaNodeSize" :key="index" class="mb10">
          <h4 class="mb5">{{item.name}}</h4>
          <div class="size-block">
            <div v-if="item.size" class="size-bar" :style="{'width':(item.size / metaNodeSize[0].size * 100)+ '%'}">
              <span class="size-val">{{item.unitSize}}</span>
            </div>
            <div v-else-if="item.size === 0 || item.size < 0">
              <span class="size-val">{{item.size}} GB</span>
            </div>
            <div v-else>
              <span class="size-val">NaN</span>
            </div>
          </div>
        </div>
      </div>
      <!--VolumeTotalSize-->
      <div class="each-block b-large-size fl">
        <h3 class="mb15">VolumeTotalSize</h3>
        <div class="overflow-block" v-if="!volumeTotalSize.data.length">
          No data
        </div>
        <ul class="overflow-block" v-else>
          <li v-for="(item, index) in volumeTotalSize.data" :key="index" class="mb10 clearfix">
            <h4 class="fl flex-h4">{{item.metric.volName}}</h4>
            <div class="size-block flex-size-block fl">
              <div v-if="item.size" class="size-bar" :style="{'width':(item.size / volumeTotalSizeMax) * 100 + '%'}">
                <span class="size-val" v-if="item.unitSize">{{item.unitSize}}</span>
              </div>
              <div v-else-if="item.size === 0">
                <span class="size-val">0 GB</span>
              </div>
              <div v-else>
                <span class="size-val">NaN</span>
              </div>
            </div>
          </li>
        </ul>
      </div>
    </div>
    <!--RATIO-->
    <div class="ratio-block clearfix">
      <div class="each-block b-min-size fl" v-for="(item, index) in ratioList1" :key="'ratioList1'+index">
        <h3 class="mb30">{{item.name}}</h3>
        <div style="text-align: center" v-if="item.size">
          <el-progress type="dashboard" :percentage="item.size" :stroke-width="10" :color="colors"></el-progress>
        </div>
        <div v-else class="no-data">
          -
        </div>
      </div>
      <div class="each-block b-min-size fl" v-for="(item, index) in ratioList2" :key="'ratioList2'+index">
        <h3 class="mb30">{{item.name}}</h3>
        <div class="light-block light-block-error" v-if="item.size">
          <div class="light-block-con">{{item.size}}</div>
        </div>
        <div class="light-block" v-else>
          <div class="light-block-con">0</div>
        </div>
      </div>
      <!--VolumeUsedRatio-->
      <div class="each-block b-small-size volume-used-ratio fl">
        <h3 class="mb15">VolumeUsedRatio</h3>
        <div class="overflow-block" v-if="!volumeRatio.data.length">
          No data
        </div>
        <ul class="overflow-block" v-else>
          <li v-for="(item, index) in volumeRatio.data" :key="index" class="mb10 clearfix">
            <h4 class="fl flex-h4">{{item.metric.volName}}</h4>
            <div v-if="item.ratio" class="size-block flex-size-block fl">
              <div class="size-bar" :style="{'width':( item.ratio) + '%'}">
                <span class="size-val">{{item.ratio}} %</span>
              </div>
            </div>
            <div v-else>
            <span class="size-val">NaN</span>
            </div>
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script>
import echarts from 'echarts/lib/echarts'
import 'echarts/lib/component/legend'
import 'echarts/lib/chart/line'
// 引入提示框和标题组件
import 'echarts/lib/component/tooltip'
import 'echarts/lib/component/title'
import 'echarts/lib/component/dataZoom'
import {echartResize} from '../../assets/echartResize.js'
import baseGql from '../../graphql/dashboard'
import clusterSel from '../../components/dashboard/cluster'
import masterInstanceSel from '../../components/dashboard/masterInstance'
import topkSel from '../../components/dashboard/topK'
export default {
  name: 'clusters',
  components: {
    clusterSel,
    masterInstanceSel,
    topkSel
  },
  data () {
    return {
      typeVal: 'All',
      typeList: [
        'All',
        'MasterCount'
      ],
      resourceChart: null,
      timeInterval: ['Daily', 'Weekly', 'Monthly'],
      totalTimeInterval: 'Daily',
      startTime: null,
      endTime: null,
      step: 15,
      appVal: 'cfs',
      clusterVal: null,
      masterInstanceVal: null,
      topkVal: null,
      countList: [
        {
          id: 'MasterCount',
          text: 'MasterCount',
          query: 'count(up{cluster="$cluster", app="$app", role="master"} > 0)',
          data: [],
          isLoading: true,
          num: '-'
        },
        {
          id: 'MetaNodeCount',
          text: 'MetaNodeCount',
          query: 'count(up{cluster="$cluster", app="$app", role="metanode"} > 0)',
          data: [],
          isLoading: true,
          num: '-'
        },
        {
          id: 'DataNodeCount',
          text: 'DataNodeCount',
          query: 'count(up{cluster="$cluster", app="$app", role="dataNode"} > 0)',
          data: [],
          isLoading: true,
          num: '-'
        },
        {
          id: 'ClientCount',
          text: 'ClientCount',
          query: 'count(up{cluster="$cluster", app="$app", role="fuseclient"} > 0)',
          data: [],
          isLoading: true,
          num: '-'
        },
        {
          id: 'ObjectNodeCount',
          text: 'ObjectNodeCount',
          query: 'sum($app_master_vol_count{cluster=~"$cluster"})',
          data: [],
          isLoading: true,
          num: '-'
        },
        {
          id: 'VolumeCount',
          text: 'VolumeCount',
          query: 'sum($app_master_vol_count{cluster=~"$cluster"})',
          data: [],
          isLoading: true,
          num: '-'
        }
      ],
      dataNodeSize: [
        {
          name: 'TotalSize',
          query: 'cfs_master_dataNodes_total_GB{app="$app",cluster="$cluster"}',
          size: 0,
          unitSize: 0
        },
        {
          name: 'UsedSize',
          query: 'cfs_master_dataNodes_used_GB{app="$app",cluster="$cluster"}',
          size: 0,
          unitSize: 0
        },
        {
          name: 'IncreasedSize',
          query: 'cfs_master_dataNodes_increased_GB{app="$app",cluster="$cluster"}',
          size: 0,
          unitSize: 0
        }
      ],
      metaNodeSize: [
        {
          name: 'TotalSize',
          query: 'cfs_master_metaNodes_total_GB{cluster=~"$cluster"}',
          size: 0,
          unitSize: 0
        },
        {
          name: 'UsedSize',
          query: 'cfs_master_metaNodes_used_GB{cluster=~"$cluster"}',
          size: 0,
          unitSize: 0
        },
        {
          name: 'IncreasedSize',
          query: 'cfs_master_metaNodes_increased_GB{cluster=~"$cluster"}',
          size: 0,
          unitSize: 0
        }
      ],
      volumeTotalSizeMax: 0,
      volumeTotalSize: {
        name: 'VolumeTotalSize',
        query: 'topk($topk, cfs_master_vol_total_GB{app="$app",cluster="$cluster"})',
        data: []
      },
      ratioList1: [
        {
          name: 'DataNodeUsedRatio',
          query: 'cfs_master_dataNodes_used_GB{app="$app",cluster="$cluster"}/ cfs_master_dataNodes_total_GB{app="$app",cluster="$cluster"}',
          size: 0
        },
        {
          name: 'MetaNodeUsedRatio',
          query: 'cfs_master_metaNodes_used_GB{app="$app",cluster="$cluster"}/ cfs_master_metaNodes_total_GB{app="$app",cluster="$cluster"}',
          size: 0
        }
      ],
      ratioList2: [
        {
          name: 'Active DataNodes',
          query: 'cfs_master_dataNodes_inactive{app="$app", cluster="$cluster"}',
          size: 0
        },
        {
          name: 'Active MetaNodes',
          query: 'cfs_master_metaNodes_inactive{app="$app", cluster="$cluster"}',
          size: 0
        },
        {
          name: 'DiskError',
          query: 'cfs_master_disk_error{app="$app", cluster="$cluster"}',
          size: 0
        }
      ],
      colors: [
        {color: '#188AEF', percentage: 30},
        {color: '#14B8EA', percentage: 70},
        {color: '#1FC8DC', percentage: 100}
      ],
      volumeRatio: {
        name: 'VolumeUsedRatio',
        query: 'topk($topk, cfs_master_vol_used_GB{app="$app",cluster="$cluster"}/cfs_master_vol_total_GB{app="$app",cluster="$cluster"})',
        data: []
      },
      biByte: ['iB', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'],
      byte: ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    }
  },
  methods: {
    changeTime (val) {
      this.totalTimeInterval = val || this.totalTimeInterval
      let startTime = parseInt(new Date().getTime() / 1000)
      let endTime = parseInt(new Date().getTime() / 1000)
      const eachS = 24 * 60 * 60
      switch (val) {
        case 'Daily':
          startTime = endTime - eachS
          // this.step = 30
          this.step = 900
          break
        case 'Weekly':
          this.step = 1200
          // this.step = 7200
          startTime = endTime - (eachS * 7)
          break
        case 'Monthly':
          this.step = 7200
          // this.step = 21600
          startTime = endTime - (eachS * 30)
          break
        default:
          startTime = endTime - eachS
      }
      this.startTime = startTime
      this.endTime = endTime
      this.queryCount()
    },
    topkChange (val) {
      this.topkVal = val
      this.changeTime(this.totalTimeInterval)
      this.queryData(this.volumeTotalSize)
      this.queryData(this.volumeRatio, 'ratio')
    },
    queryCount () {
      this.countList.forEach(item => {
        this.queryBaudStorage(item)
      })
    },
    queryBaudStorage (item) {
      item.isLoading = true
      const variables = {
        // query: item.query.replace('$cluster', this.clusterVal).replace('$app', this.appVal),
        query: item.query.replace('$app', this.appVal),
        start: this.startTime,
        end: this.endTime,
        step: this.step
      }
      this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
        item.isLoading = false
        if (!res.code) {
          const result = JSON.parse(res.data.RangeQuery).data.result
          const data = result && result[0] ? result[0].values : []
          item.data = data
          item.num = !data.length ? '-' : item.data[item.data.length - 1][1]
          if (item.id === 'VolumeCount') {
            const nowTime = (new Date()).getTime() / 1000
            const endTime = item.data[0] ? item.data[0][0] : nowTime
            item.num = parseInt((nowTime - endTime) / 60 / 60 / 24)
          }
          this.initCountChart(item)
        }
      }).catch((error) => {
        item.isLoading = false
        console.log(error)
      })
    },
    initCountChart (item) {
      let resourceChart = echarts.init(document.getElementById(item.id))
      let chartsId = document.getElementById(item.id)
      let option = {
        xAxis: {
          type: 'category',
          boundaryGap: false,
          axisLabel: {
            formatter: function () {
              return ''
            }
          },
          axisTick: { // x轴刻度线
            show: false
          },
          axisLine: { // x轴
            show: false
          }
        },
        yAxis: {
          type: 'value',
          axisLabel: {
            formatter: function () {
              return ''
            }
          },
          splitLine: {
            show: false
          },
          axisTick: { // y轴刻度线
            show: false
          },
          axisLine: { // y轴
            show: false
          }
        },
        grid: {
          left: '3',
          right: '10',
          bottom: '3',
          top: '0',
          containLabel: true
        },
        series: [{
          data: item.data,
          type: 'line',
          symbol: 'none',
          // smooth: 0.8,
          itemStyle: {
            normal: {
              color: 'rgba(69,168,240,0.74)', // 改变折线点的颜色
              lineStyle: {
                color: 'rgba(69,168,240,0.74)' // 改变折线颜色
              }
            }
          },
          areaStyle: {
            normal: {
              color: 'rgba(0,158,255,0.08)' // 改变区域颜色
            }
          }
        }]
      }
      resourceChart.setOption(option, true)
      let listener = function () {
        resourceChart.resize()
      }
      echartResize.on(chartsId, listener)
    },
    formatSize (unit, bytes, decimals) {
      if (bytes === 0) return '0 Bytes'
      bytes = decimals === 2 ? bytes : bytes * Math.pow(10, 9)
      const k = decimals === 2 ? 1024 : 1000
      const i = Math.floor(Math.log(bytes) / Math.log(k))
      return parseFloat((bytes / Math.pow(k, i)).toFixed(3)) + ' ' + unit[i]
    },
    queryData (item, type) {
      const variables = {
        query: item.query.replace(/\$topk/g, this.topkVal)
      }
      this.apollo.query(this.url.monitor, baseGql.query, variables).then((res) => {
        if (!res.code) {
          const result = JSON.parse(res.data.Query).data.result
          // VolumeTotalSize
          if (item.name === 'VolumeTotalSize') {
            const newDataJson = []
            result.forEach(item => {
              let size = item.value[1] ? parseInt(item.value[1]) : 0
              if (size && (size > this.volumeTotalSizeMax)) this.volumeTotalSizeMax = size
              // let unitSize = this.formatSize(this.biByte, size, 2)
              let unitSize = this.formatSize(this.byte, size, 10)
              // let unitSize = 1
              const obj = {
                'metric': item.metric,
                'time': item.value[0],
                'size': size,
                'unitSize': unitSize
              }
              newDataJson.push(obj)
            })
            item.data = newDataJson
            return
          }
          // VolumeUsedRatio
          if (item.name === 'VolumeUsedRatio') {
            const newDataJson = []
            result.forEach(item => {
              const obj = {
                'metric': item.metric,
                'time': item.value[0],
                'ratio': item.value[1] ? (item.value[1] * 100).toFixed(2) : null
              }
              newDataJson.push(obj)
            })
            item.data = newDataJson
            return
          }
          const value = result.length ? result[0].value : []
          let eachSize = 0
          if (type) {
            // ratio
            eachSize = value[1] ? Number((value[1] * 100).toFixed(2)) : null
            item.size = eachSize
          } else {
            // dataNode & metaNode
            eachSize = value[1] !== undefined ? parseInt(value[1]) : '-'
            item.size = eachSize
            let unitSize = this.formatSize(this.byte, eachSize, 10)
            item.unitSize = unitSize
          }
        }
      }).catch((error) => {
        console.log(error)
      })
    }
  },
  mounted () {
    this.dataNodeSize.forEach(item => {
      this.queryData(item)
    })
    this.metaNodeSize.forEach(item => {
      this.queryData(item)
    })
    this.ratioList1.forEach(item => {
      this.queryData(item, 'ratio')
    })
    this.ratioList2.forEach(item => {
      this.queryData(item, 'ratio')
    })
    // const timer = setInterval(function () {
    //   _this.changeTime(_this.totalTimeInterval)
    // }, 10000)
    // this.$once('hook:beforeDestroy', () => {
    //   clearInterval(timer)
    // })
  }
}
</script>

<style scoped>
  .dash-cluster h2 {
    font-size: 14px;
    color: #1B3F80;
  }
  .dash-cluster .count {
    width: calc((100% / 6) - 10px);
    height: 155px;
    padding-top: 10px;
    border-radius: 2px;
    border: 1px solid #EBEBEB;
    box-sizing: border-box;
    margin-right: 10px;
    position: relative;
  }
  .dash-cluster .count .cont-loading{
    position: absolute;
    right:15px;
    top:15px;
  }
  .dash-cluster .count span {
    text-align: center;
    display: block;
    line-height: 22px;
    font-size: 14px;
    color: rgba(0, 0, 0, .45);
    overflow: hidden;
    word-break: keep-all;
    text-overflow: ellipsis;
  }
  .dash-cluster .count b {
    display: block;
    margin-top: 15px;
    font-size: 30px;
    text-align: center;
    color: rgba(0,0,0,0.85);
    font-weight: normal;
  }
  .dash-cluster .count b em{
    font-size: 18px;
  }
  .dash-cluster .cluster_pic{
    position: absolute;
    bottom: 0;
  }
  .dash-cluster .metrics {
    display: flex;
    flex-direction: row;
    justify-content: space-between;
  }
  .dash-cluster .metrics li {
    width: 32%;
    height: 305px;
    border-radius: 4px;
    border: 1px solid #F1F1F1;
    box-shadow: 0 2px 5px 0 rgba(85,85,85,0.13);
  }
  .total-title {
    position: relative;
    padding-left: 260px;
  }
  .total-ul {
    position: absolute;
    top: 0px;
    left: 0px;
  }
  .total-ul li {
    padding: 8px 20px;
    float: left;
    cursor: pointer;
  }
  .total-ul li.active {
    color: #fff;
    background: #009EFF;
  }
  .each-block{
    height:200px;
    background: rgba(255,255,255,1);
    border: 1px solid rgba(241,241,241,1);
    border-radius: 2px;
    box-shadow: 0px 2px 5px 0px rgba(85,85,85,0.13);
    padding: 14px;
    box-sizing: border-box;
    margin-right: 10px;
    margin-bottom: 20px;
  }
  .each-block h3{
    line-height: 17px;
    font-family: Helvetica;
    font-size: 14px;
    color: rgba(51,51,51,1);
    overflow: hidden;
    word-break: keep-all;
    text-overflow: ellipsis;
  }
  .each-block h4{
    line-height: 16px;
    font-family: Helvetica;
    font-size: 13px;
    color: rgba(51,51,51,1);
  }
  .flex-h4{
    width: 100px;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    text-align: right;
    padding-right: 10px;
    box-sizing: border-box;
    font-weight: normal;
  }
  .b-min-size{
    width: calc(100% / 7 - 11px);
  }
  .b-small-size{
    width: calc(100% / 7 * 2 - 11px)
  }
  .b-large-size{
    width: calc(100% / 7 * 3 - 11px)
  }
  .overflow-block{
    height: calc(100% - 30px);
    overflow-y: auto;
  }
  .size-block{
    padding-right: 80px;
  }
  .flex-size-block{
    width: calc(100% - 190px);
  }
  .each-block .size-bar{
    height: 14px;
    background: linear-gradient(270deg, rgba(33,204,217,1) 0%,rgba(17,179,239,1) 52%,rgba(25,137,239,1) 100%);
    border-radius: 7px;
    margin-top: 3px;
    position: relative;
  }
  .overflow-block .size-bar{
    height: 10px;
  }
  .each-block .size-bar .size-val{
    position: absolute;
    width: 80px;
    left: 100%;
    margin-left: 8px;
    font-size: 13px;
    top: -3px;
    color: rgba(0,158,255,1);
  }
  .no-data{
    font-size: 20px;
    line-height: 30px;
    text-align: center;
  }
  .light-block{
    width: 111px;
    height: 111px;
    margin: 0 auto;
    background: #fff;
    color: #333333;
    font-size: 18px;
    text-align: center;
    border-radius: 112px;
    position: relative;
    background:  linear-gradient(210deg, rgba(115,239,167,1) 0%,rgba(20,192,109,1) 100%);
  }
  .light-block-error{
    background: linear-gradient(219deg, rgba(255,151,98,1) 0%,rgba(227,46,68,1) 100%);
  }
  .light-block-con{
    width: 86px;
    height: 86px;
    line-height: 86px;
    background: #fff;
    color: #333333;
    font-size: 18px;
    text-align: center;
    border: 1px transparent solid;
    border-radius: 90px;
    position: absolute;
    top: 12px; bottom: 12px;
    left: 12px; right: 12px;
  }
  @media screen and (max-width: 1400px) {
    .dash-cluster .count b em{
      font-size: 1vw;
      line-height: 12px;
    }
    .b-min-size {
      width: calc(100% / 5 - 11px);
      min-width: 140px;
    }
    .volume-used-ratio{
      width:calc(100% - 11px)
    }
  }
</style>
