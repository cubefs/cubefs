<!-- 适用前后端分页 -->
<template>
  <div>
    <el-row class="table-container">
      <el-table ref="table" :data="dataInner" v-bind="$attrs" :header-cell-style="{ background: '#f8f9fc', color: '#606266', fontWeight: '550' }" v-on="innerListeners">
        <slot></slot>
      </el-table>
    </el-row>
    <el-row v-if="hasPage" class="pagination">
      <el-pagination
        v-if="!markerPagination"
        :current-page="currentPageInner"
        :page-sizes="pageSizes"
        :page-size="pageSizeInner"
        :layout="layouts"
        :total="totalInner"
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
      >
      </el-pagination>
      <Pagination
        v-else
        ref="pagination"
        :marker="marker"
        :page-size="pageSize"
        :page-sizes="pageSizes"
        @update:pageSize="handleSizeChange"
        @update="emitMarker"
      ></Pagination>
    </el-row>
  </div>
</template>

<script>
import { objSort } from '@/utils'
import Pagination from '@/components/Pagination.vue'
export default {
  components: {
    Pagination,
  },
  props: {
    isNeedClientPaging: {
      // 是否需要前端分页
      type: Boolean,
      default: false,
    },
    marker: {
      type: [String, Number],
      default: '',
    },
    data: {
      type: Array,
      default: () => [],
      required: true,
    },
    markerPagination: {
      type: Boolean,
      default: false,
    },
    hasPage: {
      type: Boolean,
      default: true,
    },
    currentPage: {
      type: Number,
      default: 1,
    },
    layouts: {
      type: String,
      default: () => 'total, sizes, prev, pager, next, jumper',
    },
    pageSizes: {
      type: Array,
      default: () => [5, 15, 30, 50, 100, 500],
    },
    pageSize: {
      type: Number,
      default: 15,
    },
    total: {
      type: Number,
      default: 0,
    },
    sortByOrder: { // 如果开启还需设置sortable="custom"
      type: Boolean,
      default: false,
    },
    defaultShouldSortKey: { // 默认数据需要根据什么字段排序
      type: String,
      default: '',
    },
  },
  data() {
    return {
      dataInner: [],
      dataInnerOrigin: [],
      dataInnerCopy: [...this.data], // 拷贝一份原始数据
      currentPageInner: this.currentPage,
      pageSizeInner: this.pageSize,
      totalInner: this.total || this.data.length,
      keyList: {}, // 保存上次的排序的key
    }
  },
  computed: {
    innerListeners() { // 覆盖默认的sort-change
      return !this.sortByOrder ? this.$listeners : { ...this.$listeners, 'sort-change': this.sortChange }
    },
  },
  watch: {
    currentPage(val) {
      this.currentPageInner = val
    },
    pageSize(val) {
      this.pageSizeInner = val
    },
    total(val) {
      this.totalInner = val
    },
    data(val) {
      this.dataInnerCopy = [...val]
      this.totalInner = this.isNeedClientPaging ? val.length : this.total
      this.isNeedClientPaging && (this.currentPageInner = 1)
      this.initData()
      this.$refs.table.clearSort()
    },
  },
  created() {
    this.initData()
  },
  methods: {
    emitMarker(marker) {
      this.$emit('updateMarker', marker)
    },
    clearMarker() {
      this.$refs?.pagination?.clearMarker()
    },
    sortChange({ column, prop, order }) {
      if (order) {
        this.keyList[prop] = order
        this.dataInner.sort((a, b) => {
          return objSort(a, b, this.keyList)
        })
      } else {
        this.keyList = {}
        this.dataInner = [...this.dataInnerOrigin]
      }
    },
    initData() {
      this.getData()
    },
    getData() {
      const temp = this.isNeedClientPaging
        ? this.hasPage
          ? this.sliceData(
            this.dataInnerCopy,
            this.currentPageInner,
            this.pageSizeInner,
          )
          : [...this.dataInnerCopy]
        : [...this.dataInnerCopy]
      this.dataInner = this.defaultShouldSortKey
        ? temp.sort((a, b) => {
          return objSort(a, b, { [this.defaultShouldSortKey]: 'ascending' })
        })
        : temp
      this.dataInnerOrigin = JSON.parse(JSON.stringify(this.dataInner)) // 备份方便还原
    },
    sliceData(data, start, size) {
      return data.slice((start - 1) * size, start * size)
    },
    handleSizeChange(va) {
      if (this.sortByOrder) {
        this.keyList = {}
      }
      this.currentPageInner = 1
      this.pageSizeInner = va
      if (this.isNeedClientPaging) {
        this.getData()
      } else {
        this.$emit('update:currentPage', 1)
        this.$emit('update:pageSize', va)
        this.$emit('shouldUpdateData')
      }
    },
    handleCurrentChange(val) {
      if (this.sortByOrder) {
        this.keyList = {}
      }
      this.currentPageInner = val
      if (this.isNeedClientPaging) {
        this.getData()
      } else {
        this.$emit('update:currentPage', val)
        this.$emit('shouldUpdateData')
      }
    },
  },
}
</script>

<style lang="scss" scoped>
.table-container {
  padding: 10px 0;
}
.pagination {
  text-align: right;
}
::v-deep .el-pagination__pager-number {
  line-height: 20px;
}
</style>
