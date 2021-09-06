<template>
  <div class="dash-cluster">
    <time-interval-setter></time-interval-setter>
    <div>
      <span>TopK：</span>
      <topk-sel class="inline-block mr20"></topk-sel>
      <span>MetaNodeOp：</span>
      <meta-node-sel class="mr20"></meta-node-sel>
      <span>fuseOp：</span>
      <fuse-op-sel></fuse-op-sel>
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
      <ul class="metrics">
        <li v-for="(item, index) in metricsFuse" :key="index" v-show="typeVal === 'All' || item.tag === typeVal">
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
import fuseOpSel from '../../components/dashboard/fuseOp'
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
    fuseOpSel,
    topkSel,
    timeIntervalSetter,
    chart
  },
  data () {
    return {
      typeVal: 'All',
      topKVal: 5,
      selectItem: ['OpMetaCreateInode'],
      selectFuseItem: ['fileread'],
      topKOptions: [5, 10, 20, 30, 50, 100],
      metricsDemo: [
        {
          id: 'fuse_node_time',
          text: '{0}:Time',
          query: 'topk({0}, cfs_metanode_{1}{app="cfs", cluster="spark"})',
          data: [],
          tag: 'time',
          showTitle: true
        },
        {
          id: 'fuse_node_ops',
          text: '{0}:Ops',
          query: 'topk({0}, delta(cfs_metanode_{1}_count{app="cfs", cluster="spark"}[1m])/60)',
          data: [],
          tag: 'ops',
          showTitle: true
        }
      ],
      metricsFuseDemo: [
        {
          id: 'fuse_time',
          text: '{0}:Time',
          query: 'topk({0}, cfs_fuseclient_{1}{app="cfs", cluster="spark"})',
          data: [],
          tag: 'time',
          showTitle: true
        },
        {
          id: 'fuse_ops',
          text: '{0}:Ops',
          query: 'topk({0}, cfs_fuseclient_{1}_count{app="cfs", cluster="spark"})',
          data: [],
          tag: 'ops',
          showTitle: true
        }
      ],
      metrics: [],
      metricsFuse: [],
      timeIntervalVal: null
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
          left: '15px',
          right: '30px',
          bottom: '60px',
          top: '10px',
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
          item.option = this.initMetricsChart(item)
        }
      }).catch((error) => {
        console.log(error)
      })
    },
    metaNodeChange (val) {
      this.selectItem = val
      this.initMetrics()
    },
    fuseOpChange (val) {
      this.selectFuseItem = val
      this.initMetricsFuse()
    },
    topkChange (val) {
      this.topKVal = val
      this.initMetrics()
      this.initMetricsFuse()
    },
    timeIntervalChange (val) {
      this.timeIntervalVal = val
      this.initMetrics()
      this.initMetricsFuse()
    },
    initMetrics () {
      this.metrics = []
      this.selectItem.forEach((select, idx) => {
        this.metricsDemo.forEach(demo => {
          let item = Object.assign({}, demo)
          item.id = item.id + idx
          item.text = stringFormat(item.text, select)
          item.query = stringFormat(item.query, this.topKVal, select)
          this.queryBaudStorage(item)
          this.metrics.push(item)
        })
      })
    },
    initMetricsFuse () {
      this.metricsFuse = []
      this.selectFuseItem.forEach((select, idx) => {
        this.metricsFuseDemo.forEach(demo => {
          let item = Object.assign({}, demo)
          item.id = item.id + idx
          item.text = stringFormat(item.text, select)
          item.query = stringFormat(item.query, this.topKVal, select)
          this.queryBaudStorage(item)
          this.metricsFuse.push(item)
        })
      })
    }
  },
  mounted () {
    this.initMetrics()
    this.initMetricsFuse()
  }
}
</script>

<style scoped>
  .dash-cluster .metrics {
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
  }
  .dash-cluster .metrics li {
    width: calc(50% - 10px);
    margin-bottom: 20px;
    box-sizing: border-box;
    height: 305px;
    text-align: center;
    border-radius: 4px;
    border: 1px solid #F1F1F1;
    box-shadow: 0 2px 5px 0 rgba(85,85,85,0.13);
  }
  .dash-cluster .metrics li h4{
    padding: 14px;
    font-weight: 600;
    font-size: 14px;
    color: rgba(51,51,51,1);
  }
  .dash-cluster .metrics li .charts-block{
    height: 100%;
    visibility: hidden;
  }
  .dash-cluster .metrics li .visible{
    visibility: visible;
  }
  .cont-loading{
    text-align: center;
    font-size: 20px;
    margin-top: 100px;
  }
</style>
