<!-- 适用前后端分页 -->
<template>
  <div>
    <el-row class="table-container">
      <el-table style="width: 100%" :data="dataInner" :header-cell-style="{ background: '#f8f9fc', color: '#606266', fontWeight: '550' }" v-bind="$attrs" v-on="$listeners">
        <slot></slot>
      </el-table>
    </el-row>
    <el-row v-if="hasPage" class="pagination">
      <el-pagination
        :current-page="currentPageInner"
        :page-sizes="pageSizes"
        :page-size="pageSizeInner"
        :layout="layouts"
        :total="totalInner"
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
      >
      </el-pagination>
    </el-row>
  </div>
</template>

<script>
export default {
  props: {
    isNeedClientPaging: {
      // 是否需要前端分页
      type: Boolean,
      default: true,
    },
    data: {
      type: Array,
      default: () => [],
      required: true,
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
      default: () => [5, 10, 15, 30, 50, 100],
    },
    pageSize: {
      type: Number,
      default: 15,
    },
    total: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      dataInner: [],
      dataInnerCopy: [...this.data], // 拷贝一份原始数据
      currentPageInner: this.currentPage,
      pageSizeInner: this.pageSize,
      totalInner: this.total || this.data.length,
    }
  },
  watch: {
    data(val) {
      this.dataInnerCopy = [...val]
      this.totalInner = this.isNeedClientPaging ? val.length : this.total
      this.currentPageInner = 1
      this.initData()
    },
  },
  created() {
    this.initData()
  },
  methods: {
    initData() {
      this.getData()
    },
    getData() {
      this.dataInner = this.isNeedClientPaging
        ? this.hasPage
          ? this.sliceData(
            this.dataInnerCopy,
            this.currentPageInner,
            this.pageSizeInner,
          )
          : [...this.dataInnerCopy]
        : [...this.dataInnerCopy]
    },
    sliceData(data, start, size) {
      return data.slice((start - 1) * size, start * size)
    },
    handleSizeChange(val) {
      this.pageSizeInner = val
      if (this.isNeedClientPaging) {
        this.getData()
      } else {
        this.$emit('update:pageSize', val)
      }
    },
    handleCurrentChange(val) {
      this.currentPageInner = val
      if (this.isNeedClientPaging) {
        this.getData()
      } else {
        this.$emit('update:currentPage', val)
      }
    },
  },
}
</script>

<style>
.table-container {
  padding: 10px 0;
  width: 100%;
}

.pagination {
  text-align: right;
}
</style>
