<template>
  <ul class="total-ul mr20">
    <li v-for="item in timeInterval" :key="item" :class="{'active':  item=== totalTimeInterval}" @click="changeTime(item)">{{item}}</li>
  </ul>
</template>

<script>
import { resolvingDate } from '../../utils/dateTime.js'
export default {
  name: 'clusterSel',
  data () {
    return {
      timeInterval: ['Hourly', 'Daily', 'Weekly', 'Monthly'],
      totalTimeInterval: 'Hourly'
    }
  },
  methods: {
    initDate (startTime, endTime, timeStep) {
      let dateCon = {
        xData: [],
        xDataMap: {},
        xDataInit: []
      }
      let i = 0
      let xTime = startTime
      while (xTime < endTime) {
        if (dateCon.xDataMap[xTime] === undefined) {
          dateCon.xData.push(xTime)
          dateCon.xDataMap[xTime] = i
          let dateStr = resolvingDate(xTime * 1000)
          dateCon.xDataInit.push(dateStr)
          i++
        }
        xTime += timeStep
      }
      return dateCon
    },
    changeTime (val) {
      this.totalTimeInterval = val || this.totalTimeInterval
      let step = 15
      let startTime = parseInt(new Date().getTime() / 1000)
      let endTime = parseInt(new Date().getTime() / 1000)
      endTime = endTime - (endTime % 60)
      const eachS = 60 * 60
      let timeStep = 15
      let dateCon = {}
      switch (val) {
        case 'Hourly':
          step = 15
          startTime = endTime - eachS
          timeStep = 15 // 1hour / 15s : 4 * 60 = 240
          break
        case 'Daily':
          step = 120
          startTime = endTime - (24 * eachS)
          timeStep = 60// 1day / 1min : 24 * 60 = 1440
          break
        case 'Weekly':
          step = 1200
          startTime = endTime - (eachS * 7)
          timeStep = 600 // 7day / 10min : 7 * 24 * 6 = 1008
          break
        case 'Monthly':
          step = 7200
          startTime = endTime - (eachS * 30)
          timeStep = 1800// 1mon / 30min : 30 * 24 * 2 = 1440
          break
        default:
          step = 15
          startTime = endTime - eachS
          timeStep = 15 // 1hour / 15s : 4 * 60 = 240
          break
      }
      dateCon = this.initDate(startTime, endTime, timeStep)
      const selVal = {
        startTime: startTime,
        endTime: endTime,
        step: step,
        xData: dateCon.xData,
        xDataMap: dateCon.xDataMap,
        xDataInit: dateCon.xDataInit
      }
      this.$parent.timeIntervalChange(selVal)
    }
  },
  mounted () {
    this.changeTime()
  }
}
</script>
<style scoped>
  .total-ul{
    width: auto;
    display: inline-block;
    float: left;
  }
  .total-ul li {
    padding: 8px 20px;
    float: left;
    cursor: pointer;
  }
  .total-ul li.active {
    color: #fff;
    background: #009EFF;
  }
</style>
