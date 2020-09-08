<template>
  <div class="overview">
    <div class="cluster">
      <h2>{{ $t('chubaoFS.overview.cluster.name') }}</h2>
      <ul class="cluster-ul" v-if="clusterTxt">
        <li>{{$t('chubaoFS.overview.cluster.serverCount')}}: {{clusterTxt.serverCount}}</li>
        <li>{{$t('chubaoFS.overview.cluster.dataPartitionCount')}}: {{clusterTxt.dataPartitionCount}}</li>
        <li>{{$t('chubaoFS.overview.cluster.leaderAddr')}}: {{clusterTxt.leaderAddr}}</li>
        <li>{{$t('chubaoFS.overview.cluster.metaPartitionCount')}}: {{clusterTxt.metaPartitionCount}}</li>
        <li>{{$t('chubaoFS.overview.cluster.disableAutoAlloc')}}: {{clusterTxt.disableAutoAlloc}}</li>
        <li>{{$t('chubaoFS.overview.cluster.volumeCount')}}: {{clusterTxt.volumeCount}}</li>
      </ul>
      <ul class="cluster-ul" v-else>
        <li>{{$t('chubaoFS.overview.cluster.serverCount')}}: -</li>
        <li>{{$t('chubaoFS.overview.cluster.dataPartitionCount')}}: -</li>
        <li>{{$t('chubaoFS.overview.cluster.leaderAddr')}}: -</li>
        <li>{{$t('chubaoFS.overview.cluster.metaPartitionCount')}}: -</li>
        <li>{{$t('chubaoFS.overview.cluster.disableAutoAlloc')}}: -</li>
        <li>{{$t('chubaoFS.overview.cluster.volumeCount')}}: -</li>
      </ul>
    </div>
    <div class="cluster mt20">
      <div class="total-title">
        <h2>{{$t('chubaoFS.overview.DataSize')}}</h2>
        <ul class="total-ul">
          <li v-for="(val,key) in timeInterval" :key="key" :class="{'active':  key=== totalTimeInterval}" @click="changeTime(1,key)">{{val}}</li>
        </ul>
      </div>
      <div id="total" class="total-chart"></div>
    </div>
    <div class="cluster mt20">
      <div class="total-title">
        <h2>{{$t('chubaoFS.overview.VolumeCount')}}</h2>
        <ul class="total-ul">
          <li v-for="(val,key) in timeInterval" :key="key" :class="{'active':  key=== volTimeInterval}" @click="changeTime(2,key)">{{val}}</li>
        </ul>
      </div>
      <div id="volume" class="total-chart"></div>
    </div>
    <div class="mt20">
      <el-row :gutter="20">
        <el-col :span="8">
          <div class="cluster">
            <div class="data-title">
              <h2>{{$t('chubaoFS.overview.Users')}}</h2>
              <router-link to="/userDetails">{{$t('chubaoFS.overview.Details')}}</router-link>
            </div>
            <div id="Users" class="data-chart mt30"></div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="cluster">
            <div class="data-title">
              <h2>{{$t('chubaoFS.overview.DataNode')}}</h2>
            </div>
            <div id="DataNode" class="data-chart mt30"></div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="cluster">
            <div class="data-title">
              <h2>{{$t('chubaoFS.overview.MetaNode')}}</h2>
            </div>
            <div id="MetaNode" class="data-chart mt30"></div>
          </div>
        </el-col>
      </el-row>
    </div>
  </div>
</template>

<script>
import echarts from 'echarts/lib/echarts'
import 'echarts/lib/component/legend'
import 'echarts/lib/chart/line'
import 'echarts/lib/chart/pie'
// 引入提示框和标题组件
import 'echarts/lib/component/tooltip'
import 'echarts/lib/component/title'
import 'echarts/lib/component/dataZoom'
import {echartResize} from '../../assets/echartResize.js'
import baseGql from '../../graphql/overview'
// import { resolvingDate } from '../../utils/dateTime.js'
import { formatSize } from '../../utils/string.js'
export default {
  name: 'overview',
  data () {
    return {
      resourceChart: null,
      clusterTxt: null,
      timeInterval: this.$t('chubaoFS.timeInterval'),
      totalTimeInterval: 'Daily',
      volTimeInterval: 'Daily'
    }
  },
  watch: {
    '$i18n.locale' () {
      this.timeInterval = this.$t('chubaoFS.timeInterval')
      this.changeTime(1, this.totalTimeInterval)
      this.changeTime(2, this.volTimeInterval)
      this.queryUsers()
      this.queryDataNode()
      this.queryMetaNode()
    }
  },
  methods: {
    clusterInfo () {
      this.apollo.query(this.url.cluster, baseGql.ClusterView, {}).then((res) => {
        if (res) {
          const data = res.data
          this.clusterTxt = data.clusterView
        } else {
        }
      })
    },
    initPieChart (divId, total, used) {
      let resourceChart = echarts.init(document.getElementById(divId))
      let chartsId = document.getElementById(divId)
      const totalUnit = formatSize((total * Math.pow(10, 9)), 10)
      const usedUnit = formatSize((used * Math.pow(10, 9)), 10)
      const name = divId === 'DataNode' ? this.$t('chubaoFS.overview.DataNode') : this.$t('chubaoFS.overview.MetaNode')
      let option = {
        title: {
          text: this.$t('chubaoFS.overview.TotalCapacity') + totalUnit + '  ' + this.$t('chubaoFS.overview.Size') + usedUnit,
          left: 'left',
          textStyle: {
            color: '#333',
            fontSize: 14
          }
        },
        tooltip: {
          trigger: 'item',
          confine: true,
          formatter: function (param) {
            const valUnit = formatSize((param.data.value * Math.pow(10, 9)), 10)
            return param.seriesName + '<br/>' + param.marker + param.data.name + ': ' + valUnit + '(' + param.percent + '%)'
          }
        },
        color: ['#8ACEFF', '#0981CB', '#259BEC', '#27C7F6'],
        legend: {
          left: 'center',
          top: 'bottom',
          icon: 'circle',
          data: [this.$t('chubaoFS.overview.Available'), this.$t('chubaoFS.overview.Used')]
        },
        series: [
          {
            name: name,
            type: 'pie',
            radius: ['40%', '60%'],
            center: ['50%', '50%'],
            avoidLabelOverlap: false,
            label: {
              show: false,
              position: 'center'
            },
            emphasis: {
              label: {
                show: true,
                fontSize: '14',
                fontWeight: 'bold'
              }
            },
            labelLine: {
              show: false
            },
            data: [
              {value: (total - used), name: this.$t('chubaoFS.overview.Available')},
              {value: used, name: this.$t('chubaoFS.overview.Used')}
            ]
          }
        ]
      }
      resourceChart.setOption(option, true)
      let listener = function () {
        resourceChart.resize()
      }
      echartResize.on(chartsId, listener)
    },
    initPieChartUsers (divId, data, legendArr) {
      let resourceChart = echarts.init(document.getElementById(divId))
      let chartsId = document.getElementById(divId)
      let option = {
        title: {
          text: this.$t('chubaoFS.overview.Top10Users'),
          left: 'left',
          textStyle: {
            color: '#333',
            fontSize: 14
          }
        },
        tooltip: {
          trigger: 'item',
          confine: true,
          formatter: function (param) {
            return param.seriesName + '<br/>' + param.marker + param.data.name + ': ' + param.percent + '%'
          }
        },
        color: [
          '#8ACEFF',
          '#0981CB',
          '#259BEC',
          '#27C7F6',
          '#22E1EE',
          '#2ADBB3',
          '#95D874',
          '#F2B233',
          '#F28233',
          '#EC327D'
        ],
        legend: {
          left: 'center',
          top: 'bottom',
          icon: 'circle',
          data: legendArr
        },
        series: [
          {
            name: this.$t('chubaoFS.overview.Top10Users'),
            type: 'pie',
            // radius: ['30%', '50%'],
            radius: ['40%', '60%'],
            center: ['50%', '40%'],
            avoidLabelOverlap: false,
            label: {
              show: false,
              position: 'center'
            },
            emphasis: {
              label: {
                show: true,
                fontSize: '14',
                fontWeight: 'bold'
              }
            },
            labelLine: {
              show: false
            },
            data: data
          }
        ]
      }
      resourceChart.setOption(option, true)
      let listener = function () {
        resourceChart.resize()
      }
      echartResize.on(chartsId, listener)
    },
    queryUsers () {
      const variables = {
        n: 10
      }
      this.apollo.query(this.url.user, baseGql.topNUser, variables).then((res) => {
        if (res) {
          const data = res.data.topNUser.slice(0, 10)
          const newDataArr = []
          const legendArr = []
          const xText = []
          for (let eachItem of data) {
            const newObj = {
              name: eachItem.name.substring(0, 10),
              value: eachItem.ratio
            }
            newDataArr.push(newObj)
            xText.push(eachItem.name + ': ' + eachItem.ratio * 100 + '%')
            legendArr.push(eachItem.name.substring(0, 10))
          }
          this.initPieChartUsers('Users', newDataArr, legendArr)
        } else {
        }
      }).catch((error) => {
        console.log(error)
      })
    },
    queryDataNode () {
      const variables = {
        queryTotal: 'avg(cfs_master_dataNodes_total_GB{app="$app",cluster="$cluster"})',
        queryUsed: 'avg(cfs_master_dataNodes_used_GB{app="$app",cluster="$cluster"})'
      }
      this.apollo.query(this.url.monitor, baseGql.dataNode, variables).then((res) => {
        if (res) {
          const data = res.data
          let total = JSON.parse(data.total).data.result[0].value[1] ? JSON.parse(data.total).data.result[0].value[1] : 0
          let used = JSON.parse(data.used).data.result[0].value[1] ? JSON.parse(data.used).data.result[0].value[1] : 0
          this.initPieChart('DataNode', total, used)
        } else {
        }
      }).catch((error) => {
        console.log(error)
      })
    },
    queryMetaNode () {
      const variables = {
        queryTotal: 'avg(cfs_master_metaNodes_total_GB{app="$app", cluster="$cluster"})',
        queryUsed: 'avg(cfs_master_metaNodes_used_GB{app="$app", cluster="$cluster" })'
      }
      this.apollo.query(this.url.monitor, baseGql.dataNode, variables).then((res) => {
        if (res) {
          const data = res.data
          const total = JSON.parse(data.total).data.result[0].value[1] ? JSON.parse(data.total).data.result[0].value[1] : 0
          const used = JSON.parse(data.used).data.result[0].value[1] ? JSON.parse(data.used).data.result[0].value[1] : 0
          this.initPieChart('MetaNode', total, used)
        } else {
        }
      }).catch((error) => {
        console.log(error)
      })
    },
    initLineTotal (data) {
      let resourceChart = echarts.init(document.getElementById('total'))
      let chartsId = document.getElementById('total')
      let option = {
        title: {
          // text: 'Total Data Volume',
          left: 'center',
          textStyle: {
            color: '#333',
            fontSize: 14
          }
        },
        color: ['#1989EF', '#2FCAD6'],
        legend: {
          data: [this.$t('chubaoFS.overview.TotalVolumeCapacity'), this.$t('chubaoFS.overview.UsedData')],
          right: '1%'
        },
        grid: {
          left: '1%',
          right: '1%',
          bottom: '3%',
          containLabel: true
        },
        tooltip: {
          trigger: 'axis',
          confine: true,
          formatter: function (params) {
            let newParams = []
            let tooltipString = []
            newParams = [...params]
            newParams.forEach((p) => {
              if (p.value === undefined) {
                const cont = p.marker + ' ' + p.seriesName + ': ' + '-' + '<br/>'
                tooltipString.push(cont)
                return
              }
              let cont = p.marker + ' ' + p.seriesName + ': ' + formatSize(p.value, 10) + '<br/>'
              tooltipString.push(cont)
            })
            return newParams[0].name + '<br>' + tooltipString.join('')
          }
        },
        toolbox: {
          feature: {
            saveAsImage: {}
          }
        },
        xAxis: {
          type: 'category',
          boundaryGap: false,
          data: data.total.time
        },
        yAxis: {
          type: 'value',
          axisLabel: {
            formatter: function (value) {
              return formatSize(value, 10)
            }
          }
        },
        series: [
          {
            name: this.$t('chubaoFS.overview.TotalVolumeCapacity'),
            type: 'line',
            smooth: 0.4,
            symbol: 'circle',
            symbolSize: '5',
            data: data.total.data
          },
          {
            name: this.$t('chubaoFS.overview.UsedData'),
            type: 'line',
            smooth: 0.4,
            symbol: 'circle',
            symbolSize: '5',
            data: data.used.data
          }
        ]
      }
      resourceChart.setOption(option, true)
      let listener = function () {
        resourceChart.resize()
      }
      echartResize.on(chartsId, listener)
    },
    changeTime (index, val) {
      let startTime = parseInt(new Date().getTime() / 1000)
      let endTime = parseInt(new Date().getTime() / 1000)
      const eachS = 24 * 60 * 60
      let step = 15
      switch (val) {
        case 'Daily':
          startTime = endTime - eachS
          break
        case 'Weekly':
          step = 1200
          startTime = endTime - (eachS * 7)
          break
        case 'Monthly':
          step = 7200
          startTime = endTime - (eachS * 30)
          break
        default:
          startTime = endTime - eachS
      }
      switch (index) {
        case 1:
          this.totalTimeInterval = val
          this.totalData(startTime, endTime, step)
          break
        case 2:
          this.volTimeInterval = val
          this.totalVolume(startTime, endTime, step)
          break
        default:
          break
      }
    },
    totalData (startTime, endTime, step) {
      const variables = {
        queryTotal: 'sum(cfs_master_vol_total_GB{app="$app",cluster="$cluster"})  / count(up{app="$app", role="master", cluster="$cluster"})',
        queryUsed: 'sum(cfs_master_vol_used_GB{app="$app",cluster="$cluster"})  / count(up{app="$app", role="master", cluster="$cluster"})',
        start: startTime,
        end: endTime,
        step: step
      }
      this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
        if (res) {
          const data = res.data
          const total = JSON.parse(data.total).data.result[0].values
          const totalProcess = this.dataProcessing(this.totalTimeInterval, total)
          const used = JSON.parse(data.used).data.result[0].values
          const usedProcess = this.dataProcessing(this.totalTimeInterval, used)
          const lineData = {
            total: totalProcess,
            used: usedProcess
          }
          this.initLineTotal(lineData)
        } else {
        }
      })
    },
    formatDate (type, millinSeconds) {
      var date = new Date(millinSeconds)
      var monthArr = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Spt', 'Oct', 'Nov', 'Dec']
      var suffix = ['st', 'nd', 'rd', 'th']

      var year = date.getFullYear() //
      var month = monthArr[date.getMonth()] //
      var ddate = date.getDate() //
      var hour = date.getHours() //
      var min = date.getMinutes() //
      var sec = date.getSeconds() //

      if (type === 'Daily') {
        return (hour < 10 ? '0' + hour : hour) + ':' + (min < 10 ? '0' + min : min) + ':' + (sec < 10 ? '0' + sec : sec)
      } else {
        if (ddate % 10 < 1 || ddate % 10 > 3) {
          ddate = ddate + suffix[3]
        } else if (ddate % 10 === 1) {
          ddate = ddate + suffix[0]
        } else if (ddate % 10 === 2) {
          ddate = ddate + suffix[1]
        } else {
          ddate = ddate + suffix[2]
        }
        return ddate + ',' + month + ',' + year
      }
    },
    dataProcessing (type, data) {
      const newDataArr = {
        time: [],
        data: []
      }
      for (let eachItem of data) {
        const timer = this.formatDate(type, (parseInt(eachItem[0]) * 1000))
        newDataArr.time.push(timer)
        newDataArr.data.push(parseInt(eachItem[1]))
      }
      return newDataArr
    },
    initLineVolume (data) {
      let resourceChart = echarts.init(document.getElementById('volume'))
      let chartsId = document.getElementById('volume')
      let option = {
        title: {
          // text: 'Total Volume (Count) ',
          left: 'center',
          textStyle: {
            color: '#333',
            fontSize: 14
          }
        },
        tooltip: {
          trigger: 'axis',
          confine: true
        },
        color: ['#F2B233'],
        legend: {
          data: [this.$t('chubaoFS.overview.VolumeCount')],
          right: '1%'
        },
        grid: {
          left: '1%',
          right: '1%',
          bottom: '3%',
          containLabel: true
        },
        toolbox: {
          feature: {
            saveAsImage: {}
          }
        },
        xAxis: {
          type: 'category',
          boundaryGap: false,
          data: data.time
        },
        yAxis: {
          type: 'value'
        },
        series: [
          {
            name: this.$t('chubaoFS.overview.VolumeCount'),
            type: 'line',
            symbol: 'circle',
            symbolSize: '5',
            data: data.data
          }
        ]
      }
      resourceChart.setOption(option, true)
      let listener = function () {
        resourceChart.resize()
      }
      echartResize.on(chartsId, listener)
    },
    totalVolume (startTime, endTime, step) {
      const variables = {
        query: 'avg(cfs_master_vol_count{app="$app", cluster="$cluster"})',
        start: startTime,
        end: endTime,
        step: step // 后端确认写死30
      }
      this.apollo.query(this.url.monitor, baseGql.rangeVolQuery, variables).then((res) => {
        if (res) {
          const data = JSON.parse(res.data.RangeQuery).data.result[0].values
          const dataProcess = this.dataProcessing(this.volTimeInterval, data)
          this.initLineVolume(dataProcess)
        } else {
        }
      })
    }
  },
  mounted () {
    this.clusterInfo()
    this.changeTime(1, this.totalTimeInterval)
    this.changeTime(2, this.volTimeInterval)
    this.queryUsers()
    this.queryDataNode()
    this.queryMetaNode()
  }
}
</script>

<style scoped>
  h2 {
    display: inline-block;
    font-size: 14px;
    color: #1B3F80;
    float: left;
  }
  .cluster-ul {
    width: 850px;
    overflow: hidden;
    margin-top: 30px;
    margin-left: 100px;
  }
  .cluster-ul li {
    width: 45%;
    line-height: 25px;
    margin-bottom: 10px;
    box-sizing: border-box;
    float: left;
  }
  .total-title {
    height: 50px;
  }
  .total-ul {
    display: inline-block;
    overflow: hidden;
    margin-top: -7px;
    margin-left: 35px;
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
  .total-chart {
    width: 100%;
    height: 300px;
  }
  .total-chart img {
    width: 100%;
    height: 100%;
  }
  .data-title {
    overflow: hidden;
  }
  .data-title a {
    color: #1B3F80;
    float: right;
  }
  .data-chart{
    width: 100%;
    height: 350px;
  }
</style>
