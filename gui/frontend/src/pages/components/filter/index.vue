<template>
  <el-row class="label">
    <label class="el-form-item__label">过滤选项:</label>
    <div class="mini f-l">
      <div v-for="item in options" :key="item.type" class="f-l">
        <label class="el-form-item__label mini mr-l">{{ item.label }}</label>
        <el-select
          v-model="filterForms[item.filterKey]"
          size="mini"
          clearable
          class="el-form-item__content f-l select"
          @change="onSelectStatus"
          @clear="onClearStatus(item)"
        >
          <el-option
            v-for="it in item.options"
            :key="it.value"
            :label="it.label"
            :value="it.value"
          ></el-option>
        </el-select>
      </div>
    </div>
  </el-row>
</template>
<script>
// 配置表(增加过滤项只需修改配置表)
const keyMap = {
  STATUS: {
    type: 'STATUS',
    label: '状态',
    filterKey: 'status',
    options: null, // 需要计算的,这里预留, value最好为字符串
    filterFuction: (item, value) => {
      // item为list里的数据项, value为filterFormskey的值
      return value === item
    },
  },
  USEDRATIO: {
    type: 'USEDRATIO',
    label: '使用率',
    filterKey: 'usage_ratio',
    options: [
      {
        label: '0%',
        value: '0',
      },
      {
        label: '50%',
        value: '50',
      },
      {
        label: '70%',
        value: '70',
      },
      {
        label: '80%',
        value: '80',
      },
    ],
    filterFuction: (item, value) => {
      return parseFloat(item) >= +value
    },
  },
  RECOVER: {
    type: 'RECOVER',
    label: 'isRecovering',
    filterKey: 'IsRecover',
    options: [
      {
        label: 'true',
        value: 'true',
      },
      {
        label: 'false',
        value: 'false',
      },
    ],
    filterFuction: (item, value) => {
      return item + '' === value
    },
  },
  VOLUMNTYPE: {
    type: 'VOLUMNTYPE',
    label: '卷类型',
    filterKey: 'vol_type',
    options: [
      {
        label: '多副本卷',
        value: 0,
      },
      {
        label: '纠删码卷',
        value: 1,
      },
    ],
    filterFuction: (item, value) => {
      // item为list里的数据项, value为filterFormskey的值
      return value === item
    },
  },
}
const getKeyMapFuc = (filterKey) => {
  for (const [key, value] of Object.entries(keyMap)) {
    if (filterKey === value.filterKey) {
      return value.filterFuction
    }
  }
  return null
}
export default {
  props: {
    dataList: {
      type: Array,
      default: () => [],
    },
    types: {
      type: Array,
      default: () => ['STATUS', 'USEDRATIO'],
    },
  },
  data() {
    return {
      originDataList: [],
      filterForms: this.initFilterForms(),
      options: this.getOptions(),
    }
  },
  computed: {},
  watch: {
    dataList(v) {
      this.originDataList = [...v]
      // 得到数据里的状态
      const temp = [...new Set((v || []).map((item) => item.status))].map(
        (item) => {
          return {
            label: item,
            value: item,
          }
        },
      )
      this.setKeyMapOpt('STATUS', temp)
    },
  },
  created() { },
  methods: {
    setKeyMapOpt(type, arr) {
      this.options.forEach((item) => {
        if (item.type === type) {
          item.options = arr
        }
      })
    },
    getOptions() {
      return Object.entries(keyMap)
        .map(([key, value]) => {
          return value
        })
        .filter((item) => this.types.includes(item.type))
    },
    initFilterForms() {
      const mp = {}
      Object.entries(keyMap).forEach(([key, value]) => {
        mp[value.filterKey] = ''
      })
      return mp
    },
    showFilterItem(type) {
      return this.types.includes(type)
    },
    clear() {
      Object.keys(this.filterForms).forEach(
        (item) => (this.filterForms[item] = ''),
      )
    },
    onSelectStatus() {
      let tempData = [...this.originDataList]
      Object.entries(this.filterForms).forEach(([key, value]) => {
        if (value) {
          tempData = tempData.filter((item) =>
            getKeyMapFuc(key)(item[key], value),
          )
        }
      })
      this.$emit('filterData', tempData)
    },
    onClearStatus(option) {
      const { filterKey } = option
      this.filterForms[filterKey] = ''
      this.onSelectStatus()
    },
  },
}
</script>
<style lang="scss" scoped>
.f-l {
  float: left;
}

.select {
  width: 120px;
}

.mr-l {
  margin-left: 10px;
}

.mini {
  font-size: 12px;
}
</style>
