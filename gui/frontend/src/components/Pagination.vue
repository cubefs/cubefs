<template>
  <div class="pagination1 color6">
    <div class="l-page">
      <el-select :value="innerPageSize" class="w-select" size="mini" @change="getPageSize">
        <el-option
          v-for="page in pageSizes"
          :key="page"
          :value="page"
          :label="`${page}条/页`"
        >
        </el-option>
      </el-select>
      <button type="button" class="w20" :disabled="currentPage === 1" title="首页" @click="onClick(1)">首页</button>
      <button
        type="button"
        class="w20"
        title="上一页"
        :disabled="currentPage === 1"
        @click="onClick(2)"
      >
        上一页
      </button>
      <!-- <div class="content"><span>{{ currentPage }}</span></div> -->
      <button class="w20" title="下一页" :disabled="!marker" @click="onClick(3)">下一页</button>
      <slot name="help"></slot>
    </div>
    <div class="more-data">
      <slot></slot>
    </div>
  </div>
</template>
<script>
export default {
  props: {
    marker: {
      type: [String, Number],
      default: '',
    },
    pageSizes: {
      type: Array,
      default: () => [10, 20, 50, 100, 200],
    },
    pageSize: {
      type: Number,
      default: 200,
    },
  },
  data() {
    return {
      innerPageSize: this.pageSize,
      currentPage: 1,
      innerMarker: this.marker,
      markerCache: [],
    }
  },
  created() {
    this.onClick(1)
  },
  methods: {
    clearMarker() {
      this.currentPage = 1
      this.markerCache = []
    },
    getPageSize(v) {
      this.innerPageSize = v
      this.markerCache = []
      this.currentPage = 1
      this.$emit('update:pageSize', v)
      this.innerMarker = ''
      this.emitMarker(this.innerMarker)
    },
    emitMarker(marker) {
      this.$emit('update', marker)
    },
    onClick(type) {
      if (type === 1) {
        this.markerCache = []
        this.currentPage = 1
        this.innerMarker = ''
      } else if (type === 2) {
        if (this.currentPage > 1) this.currentPage--
        else return
        if (this.markerCache.length) {
          this.markerCache.splice(this.currentPage - 1, 1)
          this.innerMarker = this.markerCache[this.markerCache.length - 1] || ''
        } else return
      } else if (type === 3) {
        if (!this.marker) {
          this.$message.warning('没有更多数据了')
          return
        }
        this.currentPage++
        this.markerCache.push(this.marker)
        this.innerMarker = this.marker
      }
      this.emitMarker(this.innerMarker)
    },
  },
}
</script>
<style lang="scss" scoped>
.w-select {
  width: 90px;
  margin-right: 6px;
}
.pagination1 {
  position: relative;
  font-size: 14px;
  display: flex;
  width: 100%;
  align-items: center;
  justify-content: center;
  height: 32px;
  .more-data {
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
  }
 .l-page {
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  right: 0;
   button {
     background: transparent;
   }
   button:disabled {
     cursor:not-allowed;
     background-color: transparent;
     color: #999;
   }
 }
}
.w20 {
  // width: 32px;
  // height: 32px;
  padding: auto;
  // font-weight: 600;
  // background-color: #f4f4f4;
  color: #666;
  display: flex;
  justify-content: center;
  align-items: center;
  border: 1px white solid;
  border-radius: 2px;
  cursor: pointer;
  margin: 0 4px;
  &:hover {
    color: #2fc29b;
  }
}
.content {
  margin: 0 10px;
}
</style>
