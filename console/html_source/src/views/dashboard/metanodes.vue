<template>
  <div class="dash-con">
    <time-interval-setter></time-interval-setter>
    <div>
      <span>TopK：</span>
      <topk-sel class="inline-block mr20"></topk-sel>
      <span>MetaNodeOp：</span>
      <meta-node-sel></meta-node-sel>
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
import metaNodeSel from '../../components/dashboard/metaNode'
import { resolvingDate } from '../../utils/dateTime.js'
import { stringFormat } from '../../utils/string.js'
import topkSel from '../../components/dashboard/topK'
import timeIntervalSetter from '../../components/dashboard/tiemIntervalSetter'
import dashboardParam from '../../config/dashboardParam'
import chart from '../../components/dashboard/chart'
export default {
  name: 'metanodes',
  components: {
    metaNodeSel,
    topkSel,
    timeIntervalSetter,
    chart
  },
  data () {
    return {
      typeVal: 'All',
      topKVal: 5,
      selectItem: ['OpMetaCreateInode'],
      topKOptions: [5, 10, 20, 30, 50, 100],
      metricsDemo: [
        {
          id: 'time',
          text: '{0}:Time',
          query: 'topk({0}, cfs_metanode_{1}{app="cfs", cluster="spark"})',
          data: [],
          tag: 'time',
          showTitle: true
        },
        {
          id: 'ops',
          text: '{0}:Ops',
          query: 'topk({0}, delta(cfs_metanode_{1}_count{app="cfs", cluster="spark"}[1m])/60)',
          data: [],
          tag: 'ops',
          showTitle: true
        }
      ],
      metrics: [],
      timeIntervalVal: null,
      isInit: true
    }
  },
  methods: {
    initMetricsChart (item) {
      return {
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
    },
    queryBaudStorage (item) {
      const that = this
      const variables = {
        query: item.query,
        start: this.timeIntervalVal.startTime,
        end: this.timeIntervalVal.endTime,
        step: this.timeIntervalVal.step
      }
      this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
        if (!res.code) {
          const data = JSON.parse(res.data.RangeQuery).data.result
          item.data = data
          let dateArr = this.timeIntervalVal.xData
          // 填充series
          let series = []
          item.data.forEach(item_ => {
            let obj = {
              name: item_.metric.instance,
              type: 'line',
              stack: item_.metric.instance
            }
            let data = []
            dateArr.forEach(xAxis => {
              let value = 0
              for (let i = 0, length = item_.values.length; i < length; i++) {
                const data_ = item_.values[i]
                if (xAxis === data_[0]) {
                  if (item.tag === 'time') {
                    value = parseFloat((Number(data_[1]) / 1000 / 1000).toFixed(2))
                  } else if (item.tag === 'ops') {
                    value = parseFloat(Number(data_[1]).toFixed(2))
                  }
                  break
                }
              }
              data.push(value)
            })
            obj.data = data
            series.push(obj)
          })
          item.series = series
          let xAxisItem = []
          dateArr.forEach(item => {
            let dateStr = resolvingDate(item * 1000)
            if (xAxisItem.indexOf(dateStr) === -1) {
              xAxisItem.push(dateStr)
            }
          })
          item.xData = xAxisItem
          item.showTitle = false
          item.option = that.initMetricsChart(item)
        }
      }).catch((error) => {
        console.log(error)
      })
    },
    metaNodeChange (val) {
      this.selectItem = val
      if (this.isInit) {
        return
      }
      this.initMetrics()
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
    this.initMetrics()
    this.isInit = false
  }
}
</script>
