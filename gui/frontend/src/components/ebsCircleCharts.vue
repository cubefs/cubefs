<template>
  <div
    ref="echartsView"
    :class="[noData ? 'hiddCenter' : '', 'width_height100']"
  >
    <div v-if="noData">{{ data.panel }}暂无数据</div>
  </div>
</template>

<script>
import Echarts from 'echarts'

export default {
  props: {
    eData: {
      type: Array,
      default: () => [],
    },
    title: {
      type: String,
      default: '',
    },
    subtext: {
      type: String,
      default: '',
    },
    valueFormat: {
      type: Function,
      default: (val) => val,
    },
    legendDirection: {
      type: String,
      default: 'vertical',
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
  async mounted() {
    this.init()
  },
  methods: {
    init() {
      if (!this.eData.length) return
      const option = {
        tooltip: {
          trigger: 'item',
          formatter: (params) => {
            let str = params.seriesName + '<br />'
            str +=
              params.marker +
              params.name +
              ': ' +
              this.valueFormat(params.value) +
              '(' +
              params.percent +
              '%)' +
              '<br />'
            return str
          },
        },

        legend: {
          bottom: '0',
          icon: 'circle',
          itemWidth: 12,
          itemHeight: 12,
          formatter: (name) => {
            const itemValue = this.eData.filter((el) => el.name === name)
            return `${name}：${this.valueFormat(itemValue[0].value)}`
          },
        },
        color: ['#8BC574', '#E5C33B', '#7f94d4', '#A4D2D5', '#FF9F69'],
        title: {
          left: 'center',
          text: this.title,
          subtext: this.subtext,
          textStyle: {
            color: '#979797',
            fontSize: 14,
            fontWeight: 'normal',
          },
        },
        calculable: true,
        realtime: true,
        grid: {
          left: '3%',
          right: '8%',
          bottom: '14%',
          containLabel: true,
        },
        series: [
          {
            name: this.title,
            type: 'pie',
            radius: '50%',
            data: this.eData,
            itemStyle: {
              // 白色缝隙
              normal: {
                borderWidth: 1,
                borderColor: '#ffffff',
              },
              emphasis: {
                borderWidth: 0,
                shadowBlur: 10,
                shadowOffsetX: 0,
                shadowColor: 'rgba(0, 0, 0, 0.5)',
              },
            },
            label: {
              normal: {
                position: 'inner',
                show: false,
              },
            },
          },
        ],
      }
      const echart = Echarts.init(this.$refs.echartsView)
      echart.clear()
      echart.setOption(option, true)
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
