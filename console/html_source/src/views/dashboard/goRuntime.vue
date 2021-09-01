<template>
  <div class="dash-cluster">
    <div>
      <time-interval-setter></time-interval-setter>
      <span class="ml15">TopK:</span>
      <topk-sel class="inline-block ml10"></topk-sel>
    </div>
    <div class="mt20">
      <ul class="metrics">
        <li v-for="(item, index) in allList" :key="index + 'time'">
          <h4>
            {{item.text}}
            <i class="el-icon-loading" v-show="item.isLoading"></i>
          </h4>
          <p class="cont-loading" v-show="!item.data.length">no data</p>
          <div :id="item.id" class="charts-block" :class="{'visible': item.data.length}"></div>
        </li>
      </ul>
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
import baseGql from '../../graphql/dashboard'
import {echartResize} from '../../assets/echartResize.js'
import { time2Str } from '../../utils/dateTime.js'
import timeIntervalSetter from '../../components/dashboard/tiemIntervalSetter'
import topkSel from '../../components/dashboard/topK'
export default {
  name: 'metanodes',
  components: {
    timeIntervalSetter,
    topkSel
  },
  data () {
    return {
      allList: [
        {
          id: 'master_nodes_invalide',
          text: 'master_nodes_invalide',
          query: 'topk($topk, 1 - up{cluster="$cluster", role="master"}  )',
          data: [],
          isLoading: true,
          num: '-'
        },
        {
          id: 'spark_metanode_inactive',
          text: 'spark_metanode_inactive',
          query: 'topk($topk, 1 - up{cluster="$cluster", role="metanode"} > 0 )',
          data: [],
          isLoading: true,
          num: '-'
        },
        {
          id: 'datanode_inactive',
          text: 'datanode_inactive',
          query: 'topk($topk, 1 - up{cluster="$cluster", role="dataNode"}  > 0)',
          data: [],
          isLoading: true,
          num: '-'
        },
        {
          id: 'fuseclient_inactive',
          text: 'fuseclient_inactive',
          query: 'topk($topk, 1 - up{app="$app", cluster="$cluster", role="fuseclient"}  > 0)',
          data: [],
          isLoading: true,
          num: '-'
        }
      ],
      topkVal: null
    }
  },
  methods: {
    timeIntervalChange (val) {
      this.timeIntervalVal = val
      this.queryData()
    },
    topkChange (val) {
      this.topkVal = val
      this.queryData()
    },
    queryData () {
      if (this.timeIntervalVal && this.topkVal) {
        this.allList.forEach(item => {
          item.isLoading = true
          this.queryAllData(item)
        })
      }
    },
    queryAllData (item) {
      const variables = {
        start: this.timeIntervalVal.startTime,
        end: this.timeIntervalVal.endTime,
        step: this.timeIntervalVal.step,
        query: item.query.replace(/\$topk/g, this.topkVal)
      }
      this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
        item.isLoading = false
        if (!res.code) {
          const data = JSON.parse(res.data.RangeQuery).data.result
          item.data = data
          this.initData(item)
        }
      }).catch((error) => {
        item.isLoading = false
        console.log(error)
      })
    },
    initData (data) {
      const itemNew = Object.assign({}, data)
      let dateArr = []
      itemNew.data.forEach(x => {
        // 提取时间
        let time = x.values[0]
        dateArr.push(time[0])
      })
      dateArr.sort(function (a, b) { return a - b })
      let xAxisItem = []
      dateArr.forEach(item => {
        let dateStr = time2Str(item * 1000)
        if (xAxisItem.indexOf(dateStr) === -1) {
          xAxisItem.push(dateStr)
        }
      })
      itemNew.xData = xAxisItem
      // 填充series
      let series = []
      itemNew.data.forEach(item_ => {
        let obj = {
          name: item_.metric.instance,
          type: 'line',
          stack: item_.metric.instance
        }
        let data = []
        itemNew.xData.forEach(xAxis => {
          let value = 0
          item_.values.forEach(data_ => {
            let dateStr = time2Str(data_[0] * 1000)
            if (xAxis === dateStr) {
              value = parseInt(data_[1])
            }
          })
          data.push(value)
        })
        obj.data = data
        series.push(obj)
      })
      itemNew.series = series
      this.initMetricsChart(itemNew)
    },
    initMetricsChart (item) {
      let resourceChart = echarts.init(document.getElementById(item.id))
      let chartsId = document.getElementById(item.id)
      let option = {
        // title: {
        //   text: item.text,
        //   left: '15px',
        //   top: '13px',
        //   textStyle: {
        //     color: '#333',
        //     fontSize: 14
        //   }
        // },
        tooltip: {
          trigger: 'axis',
          confine: true,
          formatter: function (params) {
            let newParams = []
            let tooltipString = []
            newParams = [...params]
            newParams.sort((a, b) => { return b.value - a.value })
            newParams.forEach((p) => {
              const cont = p.marker + ' ' + p.seriesName + ': ' + p.value + '<br/>'
              tooltipString.push(cont)
            })
            return tooltipString.join('')
          }
        },
        grid: {
          left: '15px',
          right: '15px',
          bottom: '40px',
          top: '15px',
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
          data: item.xData
        },
        yAxis: {
          type: 'value'
        },
        series: item.series
      }
      resourceChart.setOption(option, true)
      let listener = function () {
        resourceChart.resize()
      }
      echartResize.on(chartsId, listener)
    }
  }
}
</script>

<style scoped>
  .dash-cluster h2 {
    font-size: 14px;
    color: #1B3F80;
  }
  .dash-cluster .count {
    padding-top: 13px;
    padding-left: 24px;
    height: 142px;
    border-radius: 2px;
    border: 1px solid #EBEBEB;
  }
  .dash-cluster .count span {
    display: block;
    color: rgba(0, 0, 0, .45);
  }
  .dash-cluster .count b {
    display: block;
    margin-top: 10px;
    font-size: 30px;
    color: #000;
    font-weight: normal;
  }
  .dash-cluster .metrics {
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
  }
  .dash-cluster .metrics li {
    width: 49%;
    margin-right: 10px;
    margin-bottom: 20px;
    box-sizing: border-box;
    height: 305px;
    border-radius: 4px;
    border: 1px solid #F1F1F1;
    text-align: center;
    box-shadow: 0 2px 5px 0 rgba(85,85,85,0.13);
  }
  .dash-cluster .metrics li h4{
    padding: 14px;
    font-weight: 600;
    line-height: 17px;
    font-size: 14px;
    color: rgba(51,51,51,1);
  }
  .dash-cluster .metrics li .charts-block{
    height: calc(100% - 40px);
    visibility: hidden;
  }
  .dash-cluster .metrics li .visible{
    visibility: visible;
  }
  .cont-loading{
    font-size: 20px;
    margin-top: 100px;
  }
</style>
