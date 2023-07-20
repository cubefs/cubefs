<template>
  <div>
    <div ref="echartsView" class="width_height100"></div>
  </div>
</template>

<script>
import * as echarts from 'echarts'
import moment from 'moment'

export default {
  props: {
    eData: {
      type: Array,
      default: () => [],
    },
    chartType: {
      type: String,
      default: 'line',
      validator: (params) => {
        return ['bar', 'line'].includes(params)
      },
    },
    title: {
      type: String,
      default: '',
    },
    isNeedLegend: {
      type: Boolean,
      default: true,
    },
    isBytes: {
      type: Boolean,
      default: true,
    },
    isFlow: {
      type: Boolean,
      default: false,
    },
    isCover: {
      type: Boolean,
      default: true,
    },
    timeType: {
      type: String,
      default: 'hour',
    },
  },
  data() {
    return {
      noData: false,
    }
  },
  watch: {
    eData: {
      handler(newV, oldV) {
        this.init()
      },
      deep: true,
    },
  },
  mounted() {
    this.$nextTick(() => {
      this.init()
    })
  },
  methods: {
    init() {
      if (!this.eData.length) {
        const echart = echarts.init(this.$refs.echartsView)
        echart.clear()
        echart.setOption({
          title: {
            text: '暂无数据',
            left: 'center',
            top: 'center',
          },
        })
        return
      }
      const newArr = []
      this.eData.forEach((i) => {
        i.values.forEach((t) => {
          if (String(t[0]).indexOf('-') === -1) {
            t[0] = moment(t[0] * 1000).format(
              this.timeType === 'hour' ? 'YYYY-MM-DD HH:mm' : 'YYYY-MM-DD',
            )
          }
        })
        newArr.push({
          type: this.chartType,
          data: i.values,
          name: i.name,
          areaStyle: this.isCover ? {} : null,
          smooth: true,
        })
      })
      const BytesFn = this.isFlow
        ? this.readablizeBytesFlow
        : this.readablizeBytes
      const renderFun = this.isBytes ? BytesFn : this.readablizeUnit
      const option = {
        tooltip: {
          trigger: 'axis',
          formatter: (params) => {
            let str =
              '时间：' +
              (this.timeType === 'hour'
                ? params[0].name
                : moment(params[0].name).format('YYYY-MM-DD')) +
              '<br />'
            params.forEach((p) => {
              str +=
                p.marker + p.seriesName + '  ' + renderFun(p.data[1]) + '<br />'
            })
            return str
          },
          axisPointer: {
            type: this.chartType === 'line' ? 'line' : 'shadow',
          },
        },
        legend: {
          data: newArr.map((i) => i.name),
          bottom: 'bottom',
        },
        title: {
          left: 'center',
          text: this.title,
        },
        color: [
          '#00c9c9',
          '#91cc75',
          '#fac858',
          '#ee6666',
          '#73c0de',
          '#3ba272',
          '#fc8452',
          '#9a60b4',
          '#ea7ccc',
        ],
        calculable: true,
        realtime: true,
        grid: {
          left: '3%',
          right: '8%',
          bottom: '14%',
          containLabel: true,
        },
        xAxis: {
          type: 'category',
          boundaryGap: false,
          axisTick: { show: false },
        },
        yAxis: {
          type: 'value',
          axisLine: { show: false },
          axisTick: { show: false },
          axisLabel: {
            formatter: (value) => {
              return renderFun(value)
            },
          },
        },
        // dataZoom: [
        //   {
        //     type: 'inside'
        //   }
        // ],
        series: newArr,
        ...this.eData.newOptions,
      }
      const echart = echarts.init(this.$refs.echartsView)
      echart.clear()
      if (!this.isNeedLegend) {
        delete option.legend
      }
      echart.setOption(option, true)
    },
    readablizeBytesFlow(value) {
      // 换算流量的
      if (value === 0 || !value || value === '0') return '0 Bytes'
      let newVal = Number(value)
      const k = 1024
      const sizes = [
        'Bytes',
        'KiB',
        'MiB',
        'GiB',
        'TiB',
        'PiB',
        'EiB',
        'ZiB',
        'YiB',
      ]
      let c = Math.floor(Math.log(newVal) / Math.log(k))
      if (c <= -1) c = 0
      newVal =
        newVal / Math.pow(k, c) === 1024
          ? 1 + sizes[c + 1]
          : (newVal / Math.pow(k, c)).toFixed(2) + sizes[c]
      return newVal
    },
    readablizeBytes(value) {
      // 换算流量的
      if (value === 0 || !value || value === '0') return '0 Bytes'
      let newVal = Number(value)
      const k = 1024
      const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
      let c = Math.floor(Math.log(newVal) / Math.log(k))
      if (c <= -1) c = 0
      newVal =
        newVal / Math.pow(k, c) === 1024
          ? 1 + sizes[c + 1]
          : (newVal / Math.pow(k, c)).toFixed(2) + sizes[c]
      return newVal
    },
    readablizeUnit(value) {
      if (!value) return '0'
      // 满足千万就转成亿单位
      if (Number(value) > 10000) return Number(value / 10000).toFixed(2) + '万'
      if (Number(value) > 10000000) {
        return Number(value / 100000000).toFixed(2) + '亿'
      }
      return value
    },
  },
}
</script>
<style lang="scss" scoped>
.width_height100 {
  width: 100%;
  height: 100%;
}
</style>
