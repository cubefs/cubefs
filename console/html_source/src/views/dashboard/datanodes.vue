<template>
  <div class="dash-con">
    <time-interval-setter></time-interval-setter>
    <div>
      <span>TopK：</span>
      <topk-sel class="inline-block mr20"></topk-sel>
      <span>DataNodeOp：</span>
      <data-node-sel></data-node-sel>
    </div>
    <div class="mt20">
      <ul class="metrics">
        <li v-for="(item, index) in metrics" :key="index" v-show="typeVal === 'All' || item.tag === typeVal">
          <h4>
            {{item.text}}
            <i class="el-icon-loading" v-show="item.showTitle"></i>
          </h4>
          <p class="cont-loading" v-show="item.showTitle && !item.data.length">Loading...</p>
          <p class="cont-loading" v-show="!item.showTitle &&!item.data.length">no data</p>
          <!--<div :id="item.id" class="charts-block" :class="{'visible': item.data.length}"></div>-->
          <chart :id="item.id"
                 class="charts-block"
                 :class="{'visible': item.data.length}"
                 :option="item.option">
          </chart>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import baseGql from '../../graphql/dashboard'
import dataNodeSel from '../../components/dashboard/dataNode'
import { stringFormat, formatTime } from '../../utils/string.js'
import topkSel from '../../components/dashboard/topK'
import timeIntervalSetter from '../../components/dashboard/tiemIntervalSetter'
import dashboardParam from '../../config/dashboardParam'
import chart from '../../components/dashboard/chart'
export default {
  name: 'metanodes',
  components: {
    dataNodeSel,
    topkSel,
    timeIntervalSetter,
    chart
  },
  data () {
    return {
      typeVal: 'All',
      topKVal: 5,
      selectItem: ['OpCreateExtent'],
      topKOptions: [5, 10, 20, 30, 50, 100],
      metricsDemo: [
        {
          id: 'node_time',
          text: '{0}:Time',
          query: 'topk({0}, cfs_dataNode_{1}{app="cfs", cluster="spark"})',
          data: [],
          tag: 'time',
          showTitle: true
        },
        {
          id: 'node_ops',
          text: '{0}:Ops',
          query: 'topk({0}, delta(cfs_dataNode_{1}_count{app="cfs", cluster="spark"}[1m])/60)',
          data: [],
          tag: 'ops',
          showTitle: true
        }
      ],
      metrics: [],
      timeIntervalVal: null,
      isInit: true,
      xObj: {}
    }
  },
  methods: {
    initMetricsChart (item) {
      return {
        animation: false,
        progressive: true,
        color: dashboardParam.colorLists, // 修改曲线颜色
        tooltip: {
          trigger: 'axis',
          confine: true,
          formatter: function (params) {
            let newParams = []
            let tooltipString = []
            newParams = [...params]
            newParams.sort((a, b) => {
              if (!a.value) a.value = 0.00
              if (!b.value) b.value = 0.00
              return b.value - a.value
            })
            newParams.forEach((p) => {
              if (p.value === 0.00) {
                const cont = p.marker + ' ' + p.seriesName + ': ' + '-' + '<br/>'
                tooltipString.push(cont)
                return
              }
              let cont = p.marker + ' ' + p.seriesName + ': ' + p.value + '<br/>'
              if (item.tag === 'time') {
                cont = p.marker + ' ' + p.seriesName + ': ' + formatTime(p.value, true) + '<br/>'
              }
              tooltipString.push(cont)
            })
            return newParams[0].name + '<br/>' + tooltipString.join('')
          }
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
          type: 'value',
          axisLabel: {
            formatter: function (value) {
              // 格式化成月/日，只在第一个刻度显示年份
              if (item.tag === 'time') {
                return formatTime(value, true)
              } else {
                return value
              }
            }
          }
        },
        series: item.series
      }
    },
    queryBaudStorage (item) {
      const variables = {
        query: item.query,
        start: this.timeIntervalVal.startTime,
        end: this.timeIntervalVal.endTime,
        step: this.timeIntervalVal.step
      }
      this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
        if (!res.code) {
          const data = JSON.parse(res.data.RangeQuery).data.result
          this.initData(item, data)
        }
      }).catch((error) => {
        console.log(error)
      })
    },
    initData (item, data) {
      item.data = data
      console.log('-----------Sta-- ' + new Date().getTime())
      let num = 0
      let series = []
      const dateArr = this.timeIntervalVal.xDataMap
      item.data.forEach(item_ => {
        let obj = {
          type: 'line',
          name: item_.metric.instance,
          stack: item_.metric.instance,
          showAllSymbol: true,
          symbol: 'circle',
          symbolSize: 2.5
        }
        const data = []
        item_.values.forEach(_val => {
          const index = dateArr[_val[0]]
          if (index !== undefined) {
            let value = _val[1] !== undefined ? _val[1] : 0.00
            if (item.tag === 'ops') {
              value = _val[1] !== undefined ? parseFloat(Number(_val[1]).toFixed(2)) : 0.00
            }
            data[index] = value
          }
          num++
        })
        obj.data = data
        series.push(obj)
      })
      console.log('num1 ' + num)
      item.xData = this.timeIntervalVal.xDataInit
      item.series = series
      item.showTitle = false
      item.option = this.initMetricsChart(item)
      console.log('End-- ' + new Date().getTime())
    },
    dataNodeChange (val) {
      this.selectItem = val
      if (this.isInit) {
        return
      }
      this.initMetrics(true)
    },
    topkChange (val) {
      this.topKVal = val
      if (this.isInit) {
        return
      }
      this.initMetrics()
    },
    timeIntervalChange (val) {
      this.timeIntervalVal = val
      if (this.isInit) {
        return
      }
      this.initMetrics()
    },
    initMetrics () {
      this.metrics = []
      this.selectItem.forEach((select, idx) => {
        this.metricsDemo.forEach(demo => {
          let item = Object.assign({}, demo)
          item.id = item.id + idx
          item.text = stringFormat(item.text, select)
          item.query = stringFormat(item.query, this.topKVal, select)
          item.option = {}
          this.queryBaudStorage(item)
          this.metrics.push(item)
        })
      })
    }
  },
  mounted () {
    this.initMetrics(true)
    this.isInit = false
  }
}
</script>
