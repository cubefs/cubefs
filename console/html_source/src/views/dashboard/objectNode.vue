<template>
  <div class="dash-con">
    <div>
      <time-interval-setter></time-interval-setter>
      <span>ObjectNodeOp:</span>
      <el-select v-model="objectNodeVal" multiple collapse-tags @change="objectNodeChange">
        <el-option
          v-for="item in objectNodeOptions"
          :key="item"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
      <span class="ml15">TopK:</span>
      <topk-sel class="inline-block ml10"></topk-sel>
    </div>
    <div class="mt20">
      <ul class="metrics">
        <li v-for="(item, index) in objectNodeVal" :key="index + 'time'">
          <h4>
            <span>{{item}}:Time</span>
            <i class="el-icon-loading" v-show="item.isLoading"></i>
          </h4>
          <div :id="item + 'Time'" style="height: calc(100% - 45px);">loading...</div>
        </li>
        <li v-for="(item, index) in objectNodeVal" :key="index + 'ops'">
          <h4>
            <span>{{item}}:Ops</span>
            <i class="el-icon-loading" v-show="item.isLoading"></i>
          </h4>
          <div :id="item + 'Ops'" style="height: calc(100% - 45px);">loading...</div>
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
import { resolvingDate } from '../../utils/dateTime.js'
import dashboardParam from '../../config/dashboardParam'
import timeIntervalSetter from '../../components/dashboard/tiemIntervalSetter'
import topkSel from '../../components/dashboard/topK'
export default {
  name: 'objectNodes',
  components: {
    timeIntervalSetter,
    topkSel
  },
  data () {
    return {
      objectNodeOptions: dashboardParam.objectNodeParam,
      objectNodeVal: [dashboardParam.objectNodeParam[1]],
      timeIntervalVal: null,
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
    objectNodeChange () {
      this.queryData()
    },
    queryData () {
      if (this.timeIntervalVal && this.topkVal && this.objectNodeVal.length) {
        this.objectNodeVal.forEach(item => {
          this.queryAllData(item, 'time')
          this.queryAllData(item, 'ops')
        })
      }
    },
    queryAllData (selVal, type) {
      const variables = {
        start: this.timeIntervalVal.startTime,
        end: this.timeIntervalVal.endTime,
        step: this.timeIntervalVal.step,
        query: 'topk($topk, cfs_objectnode_action_[[obOp]]{app="$app", cluster="$cluster"})'.replace(/\$topk/g, this.topkVal).replace(/\[\[obOp\]\]/g, selVal)
      }
      if (type === 'ops') variables.query = 'topk($topk, rate(cfs_objectnode_action_[[obOp]]_count{app=~"$app", cluster="$cluster"}[1m]))'.replace(/\$topk/g, this.topkVal).replace(/\[\[obOp\]\]/g, selVal)
      this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
        if (!res.code) {
          let dataAll = JSON.parse(res.data.RangeQuery).data.result
          this.initData(selVal, dataAll, type)
          if (type === 'ops') variables.query = 'sum(rate(cfs_objectnode_action_[[obOp]]_count{app=~"$app", cluster="$cluster"}[1m]))'.replace(/\$topk/g, this.topkVal).replace(/\[\[obOp\]\]/g, selVal)
          else variables.query = 'avg(cfs_objectnode_action_[[obOp]]{app="$app", cluster="$cluster"})'.replace(/\$topk/g, this.topkVal).replace(/\[\[obOp\]\]/g, selVal)
          this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
            if (!res.code) {
              const dataDesign = JSON.parse(res.data.RangeQuery).data.result[0]
              dataDesign.metric.instance = (type === 'ops') ? 'sum' : 'avg'
              dataAll.push(dataDesign)
            }
            this.initData(selVal, dataAll, type)
          }).catch((error) => {
            this.initData(selVal, dataAll, type)
            console.log(error)
          })
        }
      }).catch((error) => {
        console.log(error)
      })
    },
    initData (selVal, data, type) {
      const item = {}
      item.id = selVal + (type === 'ops' ? 'Ops' : 'Time')
      item.text = selVal + (type === 'ops' ? ':Ops' : ':Time')
      item.unit = type === 'ops' ? 'ops' : 'ms'
      item.data = data
      let dateArr = this.timeIntervalVal.xData
      // 填充series
      let series = []
      item.data.forEach(item_ => {
        let newObj = {}
        item_.values.forEach(_val => {
          const value = parseInt(_val[1])
          newObj[_val[0]] = value
        })
        let obj = {
          name: item_.metric.instance,
          type: 'line',
          stack: item_.metric.instance,
          smooth: true
        }
        let data = []
        dateArr.forEach(xAxis => {
          if (type === 'time') {
            const value = newObj[xAxis] !== undefined ? parseFloat((Number(newObj[xAxis]) / 1000 / 1000).toFixed(3)) : null
            data.push(value)
          } else {
            const value = newObj[xAxis] !== undefined ? parseFloat(Number(newObj[xAxis]).toFixed(3)) : null
            data.push(value)
          }
        })
        obj.data = data
        series.push(obj)
      })
      // item.data.forEach(item_ => {
      //   let obj = {
      //     name: item_.metric.instance,
      //     type: 'line',
      //     // smooth: 0.4,
      //     stack: item_.metric.instance
      //   }
      //   let data = []
      //   dateArr.forEach(xAxis => {
      //     let value = null
      //     for (let i = 0, length = item_.values.length; i < length; i++) {
      //       const data_ = item_.values[i]
      //       if (xAxis === data_[0]) {
      //         if (type === 'time') {
      //           value = (parseInt(data_[1]) / 1000 / 1000).toFixed(3)
      //         } else {
      //           value = parseInt(data_[1]).toFixed(3)
      //         }
      //       }
      //     }
      //     data.push(value)
      //   })
      //   obj.data = data
      //   series.push(obj)
      // })
      item.series = series
      let xAxisItem = []
      dateArr.forEach(item => {
        let dateStr = resolvingDate(item * 1000)
        if (xAxisItem.indexOf(dateStr) === -1) {
          xAxisItem.push(dateStr)
        }
      })
      item.xData = xAxisItem
      this.initMetricsChart(item)
    },
    initMetricsChart (item) {
      let resourceChart = echarts.init(document.getElementById(item.id))
      let chartsId = document.getElementById(item.id)
      let option = {
        color: dashboardParam.colorLists, // 修改曲线颜色
        tooltip: {
          trigger: 'axis',
          confine: true
        },
        grid: {
          left: '30px',
          right: '30px',
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
          splitLine: {
            show: true,
            lineStyle: {
              type: 'dashed'
            }
          },
          data: item.xData
        },
        yAxis: {
          axisTick: {
            show: false
          },
          axisLine: {
            'show': false
          },
          splitLine: {
            show: true,
            lineStyle: {
              type: 'dashed'
            }
          },
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
