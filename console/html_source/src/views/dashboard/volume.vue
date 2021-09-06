<template>
  <div class="dash-con">
    <div>
      <time-interval-setter></time-interval-setter>
      <span class="ml15">TopK:</span>
      <topk-sel class="inline-block ml10"></topk-sel>
      <span class="ml15">Vol:</span>
      <vol-sel class="inline-block ml10"></vol-sel>
    </div>
    <div class="mt20">
      <ul class="metrics all-metrics">
        <li v-for="(item, index) in allList" :key="index">
          <h4>
            <span>{{item.text}}</span>
            <i class="el-icon-loading" v-show="item.isLoading"></i>
          </h4>
          <p class="cont-loading" v-show="item.isLoading && !item.data.length">Loading...</p>
          <p class="cont-loading" v-show="!item.isLoading &&!item.data.length">no data</p>
          <chart :id="item.id"
                 class="charts-block"
                 :class="{'visible': item.data.length}"
                 :option="item.option">
          </chart>
        </li>
      </ul>
      <ul class="metrics">
        <li>
          <h4>
            <span>{{volumeSizeAndRatio.text}}</span>
            <i class="el-icon-loading" v-show="volumeSizeAndRatio.isLoading"></i>
          </h4>
          <p class="cont-loading" v-show="volumeSizeAndRatio.isLoading && !volumeSizeAndRatio.data.length">Loading...</p>
          <p class="cont-loading" v-show="!volumeSizeAndRatio.isLoading &&!volumeSizeAndRatio.data.length">no data</p>
          <chart
            :id="volumeSizeAndRatio.id"
            class="charts-block"
            :class="{'visible': volumeSizeAndRatio.data.length}"
            :option="volumeSizeAndRatio.option">
          </chart>
        </li>
        <li>
          <h4>
            <span>{{volumeRate.text}}</span>
            <i class="el-icon-loading" v-show="volumeRate.isLoading"></i>
          </h4>
          <p class="cont-loading" v-show="volumeRate.isLoading && !volumeRate.data.length">Loading...</p>
          <p class="cont-loading" v-show="!volumeRate.isLoading &&!volumeRate.data.length">no data</p>
          <chart
            :id="volumeRate.id"
            class="charts-block"
            :class="{'visible': volumeRate.data.length}"
            :option="volumeRate.option">
          </chart>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import baseGql from '../../graphql/dashboard'
import { resolvingDate } from '../../utils/dateTime.js'
import timeIntervalSetter from '../../components/dashboard/tiemIntervalSetter'
import chart from '../../components/dashboard/chart'
import topkSel from '../../components/dashboard/topK'
import volSel from '../../components/dashboard/volumeSelect'
import dashboardParam from '../../config/dashboardParam'

export default {
  name: 'dashboardVolume',
  components: {
    timeIntervalSetter,
    chart,
    topkSel,
    volSel
  },
  data () {
    return {
      allList: [
        {
          id: 'allVolumeUsedSize',
          text: 'VolumeUsedSize',
          query: 'topk($topk, cfs_master_vol_used_GB{app="$app",cluster="$cluster"})',
          data: [],
          isLoading: true,
          unit: 'TB'
        },
        {
          id: 'allVolumeUsedRatio',
          text: 'VolumeUsedRatio',
          query: 'topk($topk, cfs_master_vol_used_GB{app="$app",cluster="$cluster"}/cfs_master_vol_total_GB{app="$app",cluster="$cluster"})',
          data: [],
          isLoading: true,
          unit: '%'
        },
        {
          id: 'allVolumeSizeRate',
          text: 'VolumeSizeRate',
          query: 'topk($topk, rate(cfs_master_vol_used_GB{app="$app",cluster="$cluster"}[3m]))',
          data: [],
          isLoading: true,
          unit: 'GBs'
        }
      ],
      volumeSizeAndRatio: {
        id: 'VolumeSizeAndRatio',
        text: '',
        query1: 'cfs_master_vol_used_GB{app="$app",cluster="$cluster", volName="$vol"}',
        query2: 'cfs_master_vol_used_GB{app="$app",cluster="$cluster", volName="$vol"}/cfs_master_vol_total_GB{app="$app",cluster="$cluster", volName="$vol"}',
        data: [],
        isLoading: true
      },
      volumeRate: {
        id: 'volumeRate',
        text: '',
        query: 'rate(cfs_master_vol_used_GB{app="$app",cluster="$cluster", volName="$vol"}[1m])',
        data: [],
        isLoading: true,
        unit: 'GBs'
      },
      topkVal: null,
      volVal: null
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
    volChange (val) {
      this.volVal = val
      this.queryData()
    },
    queryData () {
      if (this.timeIntervalVal && this.topkVal) {
        this.allList.forEach(item => {
          item.isLoading = true
          this.queryAllData(item)
        })
        if (this.volVal) {
          this.queryVolRate()
          this.queryVolSizeAndRatio()
        }
      }
    },
    queryVolSizeAndRatio () {
      this.volumeSizeAndRatio.isLoading = true
      this.volumeSizeAndRatio.text = this.volVal
      const item = this.volumeSizeAndRatio
      const variables = {
        start: this.timeIntervalVal.startTime,
        end: this.timeIntervalVal.endTime,
        step: this.timeIntervalVal.step,
        query: item.query1.replace(/\$topk/g, this.topkVal).replace(/\$vol/g, this.volVal)
      }
      this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
        if (!res.code) {
          let dataAll = []
          const dataDesign = JSON.parse(res.data.RangeQuery).data.result[0] ? JSON.parse(res.data.RangeQuery).data.result[0].values : []
          dataAll.push(dataDesign)
          // if (JSON.parse(res.data.RangeQuery).data.result[0]) dataAll.push(JSON.parse(res.data.RangeQuery).data.result[0].values)
          variables.query = item.query2.replace(/\$topk/g, this.topkVal).replace(/\$vol/g, this.volVal)
          this.apollo.query(this.url.monitor, baseGql.rangeQuery, variables).then((res) => {
            item.isLoading = false
            if (!res.code) {
              const dataDesign = JSON.parse(res.data.RangeQuery).data.result[0] ? JSON.parse(res.data.RangeQuery).data.result[0].values : []
              dataAll.push(dataDesign)
            }
            item.data = dataAll
            this.initSizeAndRatioData(item)
          })
        }
      }).catch((error) => {
        item.isLoading = false
        console.log(error)
      })
    },
    queryVolRate () {
      this.volumeRate.isLoading = true
      this.volumeRate.text = this.volVal + ':rate'
      const item = this.volumeRate
      const variables = {
        start: this.timeIntervalVal.startTime,
        end: this.timeIntervalVal.endTime,
        step: this.timeIntervalVal.step,
        query: item.query.replace(/\$vol/g, this.volVal)
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
          name: item_.metric.volName,
          type: 'line',
          stack: item_.metric.volName
        }
        let data = []
        dateArr.forEach(xAxis => {
          let value = newObj[xAxis] !== undefined ? newObj[xAxis] : null
          if (itemNew.text === 'VolumeUsedRatio') value = parseFloat((value * 100).toFixed(2))
          if (itemNew.text === 'VolumeUsedSize') value = parseFloat((value / Math.pow(10, 12)).toFixed(2))
          data.push(value)
        })
        obj.data = data
        series.push(obj)
      })
      // itemNew.data.forEach(item_ => {
      //   let obj = {
      //     name: item_.metric.volName,
      //     type: 'line',
      //     stack: item_.metric.volName
      //     // areaStyle: {
      //     //   alpha: 0.1
      //     // }
      //   }
      //   let data = []
      //   dateArr.forEach(xAxis => {
      //     let value = '-'
      //     for (let i = 0, length = item_.values.length; i < length; i++) {
      //       const data_ = item_.values[i]
      //       if (xAxis === data_[0]) {
      //         value = Number(data_[1])
      //         if (itemNew.text === 'VolumeUsedRatio') value = (value * 100).toFixed(2)
      //         if (itemNew.text === 'VolumeUsedSize') value = (value / 1000)
      //         break
      //       }
      //     }
      //     data.push(value)
      //   })
      //   obj.data = data
      //   series.push(obj)
      // })
      let xAxisItem = []
      dateArr.forEach(item => {
        let dateStr = resolvingDate(item * 1000)
        if (xAxisItem.indexOf(dateStr) === -1) {
          xAxisItem.push(dateStr)
        }
      })
      itemNew.xData = xAxisItem
      itemNew.series = series
      data.option = this.initMetricsChart(itemNew)
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
          type: 'value',
          splitLine: {
            show: true,
            lineStyle: {
              type: 'dashed'
            }
          },
          axisTick: {
            show: false
          },
          axisLine: {
            'show': false
          },
          axisLabel: {
            formatter: '{value} ' + item.unit
          }
        },
        series: item.series
      }
    },
    initSizeAndRatioData (data) {
      const itemNew = Object.assign({}, data)
      let dateArr = this.timeIntervalVal.xData
      itemNew.xData = dateArr
      // 填充series
      let series = []
      series[0] = {
        name: 'size',
        type: 'line',
        stack: 'size',
        data: (function () {
          let data = []
          itemNew.xData.forEach(xAxis => {
            let value = 0
            for (let i = 0, length = itemNew.data[0].length; i < length; i++) {
              const data_ = itemNew.data[0][i]
              if (xAxis === data_[0]) {
                value = Number(data_[1])
                break
              }
            }
            data.push(value)
          })
          return data
        }())
      }
      series[1] = {
        name: 'ratio',
        type: 'line',
        yAxisIndex: 1,
        stack: 'ratio',
        data: (function () {
          let data = []
          itemNew.xData.forEach(xAxis => {
            let value = 0
            for (let i = 0, length = itemNew.data[1].length; i < length; i++) {
              const data_ = itemNew.data[1][i]
              if (xAxis === data_[0]) {
                value = Number(data_[1]) * 100
                break
              }
            }
            data.push(value)
          })
          return data
        }())
      }
      itemNew.series = series
      const xAxisItem = []
      dateArr.forEach(item => {
        let dateStr = resolvingDate(item * 1000)
        if (xAxisItem.indexOf(dateStr) === -1) {
          xAxisItem.push(dateStr)
        }
      })
      itemNew.xData = xAxisItem
      data.option = this.initMetricsChartSize(itemNew)
    },
    initMetricsChartSize (item) {
      return {
        color: dashboardParam.colorLists, // 修改曲线颜色
        tooltip: {
          trigger: 'axis',
          confine: true,
          showDelay: 0,
          axisPointer: {
            show: true,
            type: 'cross',
            lineStyle: {
              type: 'dashed',
              width: 1
            }
          }
        },
        legend: {
          // y: 'bottom', // 图例说明(属性)在底部显示，不写默认在顶部显示
          data: ['UsedSize', 'UsedRate'], // 属性类别
          selectedMode: 'multiple' // 选中模式
        },
        toolbox: {
          show: true,
          orient: 'vertical',
          left: 'right',
          top: 'center',
          feature: {
            mark: {show: true},
            dataZoom: {show: true},
            dataView: {show: true},
            magicType: {show: true, type: ['line', 'bar', 'stack', 'tiled']},
            restore: {show: true},
            saveAsImage: {show: true}
          }
        },
        grid: {
          left: '8%', // y轴离左侧边框边距
          right: '6%', // y轴离右侧边框边距
          bottom: '12%', // x轴离底部边框边距
          containLabel: true
        },
        calculable: true,
        xAxis: [
          {
            type: 'category',
            boundaryGap: false,
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
            data: item.xData
          }
        ],
        yAxis: [
          {
            type: 'value',
            name: 'UsedSize GB',
            // 默认以千分位显示，不想用的可以在这加一段
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
            axisLabel: { // 调整左侧Y轴刻度， 直接按对应数据显示
              show: true,
              showMinLabel: true,
              showMaxLabel: true,
              formatter: function (value) {
                return value
              }
            }
          },
          {
            type: 'value',
            name: 'UsedRate %',
            // 默认以千分位显示，不想用的可以在这加一段
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
            axisLabel: { // 调整左侧Y轴刻度， 直接按对应数据显示
              show: true,
              showMinLabel: true,
              showMaxLabel: true,
              formatter: function (value) {
                return value
              }
            }
          }
        ],
        series: [
          {
            name: 'UsedSize',
            type: 'line',
            smooth: true,
            yAxisIndex: 0, // 属性，归属左侧y轴
            data: item.series[0].data
          },
          {
            name: 'UsedRate',
            type: 'line',
            smooth: true,
            yAxisIndex: 1, // 属性，归属右侧y轴
            data: item.series[1].data

          }
        ]
      }
    }
  }
}
</script>

<style scoped>
  .dash-con .all-metrics li{
    width: calc((100% - 33px) / 3)
  }
  @media screen and (max-width: 1200px) {
    .dash-con .metrics li {
      width: 100%;
    }
  }
</style>
