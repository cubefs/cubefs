<template>
  <div :id="id"></div>
</template>

<script>
import echarts from 'echarts/lib/echarts'
import 'echarts/lib/component/legend'
import 'echarts/lib/chart/line'
// 引入提示框和标题组件
import 'echarts/lib/component/tooltip'
import 'echarts/lib/component/title'
import 'echarts/lib/component/dataZoom'
import {echartResize} from '../../assets/echartResize.js'

export default {
  name: 'Chart',
  props: {
    id: {
      type: String,
      required: true
    },
    option: {
      type: Object
    }
  },
  data () {
    return {
      chartsId: null,
      chartInstance: null
    }
  },
  watch: {
    option: {
      handler () {
        this.render()
      },
      deep: true
    }
  },
  methods: {
    render () {
      if (!this.id || !this.option) return
      this.chartInstance = echarts.init(document.getElementById(this.id))
      this.chartsId = document.getElementById(this.id)
      this.chartInstance.clear()
      this.chartInstance.setOption(this.option, true)
      echartResize.off(this.chartsId, this.resizeHandler)
      echartResize.on(this.chartsId, this.resizeHandler)
      console.log('Chart End-- ' + new Date().getTime())
    },
    resizeHandler () {
      this.chartInstance && this.chartInstance.resize()
    }
  },
  beforeDestroy () {
    this.chartInstance && this.chartInstance.dispose()
    this.chartsId && echartResize.off(this.chartsId, this.resizeHandler)
    this.chartsId = null
    this.chartInstance = null
  }
}
</script>
