<template>
  <div class="dash-con">
    <div>
      <time-interval-setter></time-interval-setter>
      <span class="ml15">TopK:</span>
      <topk-sel class="inline-block ml10"></topk-sel>
    </div>
    <div class="mt20">
      <ul class="metrics">
        <li v-for="(item, index) in allList" :key="index + 'time'" :class="{'last-li': index === 2}">
          <h4>
            {{item.text}}
            <i class="el-icon-loading" v-show="item.isLoading"></i>
          </h4>
          <p class="cont-loading" v-show="item.isLoading && !item.data.length">Loading...</p>
          <p class="cont-loading" v-show="!item.isLoading &&!item.data.length">no data</p>
          <chart :id="item.id"
                 class="charts-block"
                 :class="{'visible': item.data.length}"
                 :option="item.option">
          </chart>
          <!--<div :id="item.id" class="charts-block" :class="{'visible': item.data.length}"></div>-->
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import baseGql from '../../graphql/dashboard'
import { resolvingDate } from '../../utils/dateTime.js'
import timeIntervalSetter from '../../components/dashboard/tiemIntervalSetter'
import topkSel from '../../components/dashboard/topK'
import dashboardParam from '../../config/dashboardParam'
import chart from '../../components/dashboard/chart'
export default {
  name: 'dashboardMaster',
  components: {
    timeIntervalSetter,
    topkSel,
    chart
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
          tit: 'spark_master_',
          num: '-'
        },
        {
          id: 'spark_metanode_inactive',
          text: 'spark_metanode_inactive',
          query: 'topk($topk, 1 - up{cluster="$cluster", role="metanode"} > 0 )',
          data: [],
          isLoading: true,
          tit: 'metanode_',
          num: '-'
        },
        {
          id: 'datanode_inactive',
          text: 'datanode_inactive',
          query: 'topk($topk, 1 - up{cluster="$cluster", role="dataNode"}  > 0)',
          data: [],
          isLoading: true,
          tit: 'datanode_',
          num: '-'
        }
        // {
        //   id: 'fuseclient_inactive',
        //   text: 'fuseclient_inactive',
        //   query: 'topk($topk, 1 - up{app="$app", cluster="$cluster", role="fuseclient"}  > 0)',
        //   data: [],
        //   isLoading: true,
        //   num: '-'
        // }
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
      let dateArr = this.timeIntervalVal.xData
      // 填充series
      let series = []
      itemNew.data.forEach(item_ => {
        let newObj = {}
        item_.values.forEach(_val => {
          const value = parseInt(_val[1])
          newObj[_val[0]] = value
        })
        let obj = {
          name: itemNew.tit + item_.metric.instance,
          type: 'line',
          stack: item_.metric.instance
        }
        let data = []
        dateArr.forEach(xAxis => {
          let value = newObj[xAxis] !== undefined ? newObj[xAxis] : null
          data.push(value)
        })
        obj.data = data
        series.push(obj)
      })
      itemNew.series = series
      let xAxisItem = []
      dateArr.forEach(item => {
        let dateStr = resolvingDate(item * 1000)
        if (xAxisItem.indexOf(dateStr) === -1) {
          xAxisItem.push(dateStr)
        }
      })
      itemNew.xData = xAxisItem
      data.option = this.initMetricsChart(itemNew)
      // this.initMetricsChart(itemNew)
    },
    initMetricsChart (item) {
      return {
        tooltip: {
          trigger: 'axis',
          confine: true
        },
        color: dashboardParam.colorLists, // 修改曲线颜色
        grid: {
          left: '25px',
          right: '25px',
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
    }
  }
}
</script>
