<template>
  <div class="chart_warp">
    <div class="text_title">{{ title }}</div>
    <div
      ref="echartsView"
      :class="[noData ? 'hiddCenter' : '', 'width_height100']"
    >
      <div v-if="noData">{{ data.panel }}暂无数据</div>
    </div>
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
    color: {
      type: Array,
      default: () => ['#FF5C00', '#00DAB3'],
    },
  },
  data() {
    return {
      noData: false,
      total: 0,
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
    this.getTotal()
    this.init()
  },
  methods: {
    getTotal() {
      for (let i = 0; i < this.eData.length; i++) {
        this.total += this.eData[i].value
      }
    },
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
            if (this.total === 0) {
              return `${name}：(${this.valueFormat(itemValue[0].value)}/0%)`
            }
            const percent = (itemValue[0].value / this.total * 100).toFixed(2)
            return `${name}：(${this.valueFormat(itemValue[0].value)}/${percent}%)`
          },
        },
        color: this.color,
        // title: {

        //   left: 'center',
        //   text: this.title,
        //   subtext: this.subtext,
        //   textStyle: {
        //     color: '#979797',
        //     fontSize: 14,
        //     fontWeight: 'normal',
        //   },
        // },
        title: {
          text: '总量',
          subtext: this.valueFormat(this.total),
          left: 'center', // 对齐方式居中
          top: '40%', // 距离顶部
          textStyle: {
            color: 'rgb(50,197,233)', // 文字颜色
            fontSize: 14, // 字号
            align: 'center', // 对齐方式
          },
          subtextStyle: {
            fontSize: 14,
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
            radius: ['40%', '60%'],
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
      const that = this
      echart.on('click', function (params) {
        that.$emit('showDialog', params.seriesName)
      })
    },
  },
}
</script>
<style lang="scss" scoped>
.width_height100 {
  // width: 300px;
  height: 230px;
  width:300px;
}
.chart_warp {
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-bottom: 30px;
}
.text_title {
  color: #979797;
  font-size: 14px;
  margin-bottom: -30px;
}
</style>
