<template>
  <div class="card" :style="{ background: background }">
    <div class="flex">
      <slot></slot>
      <div :class="{ flex: isRow }">
        <div class="title-1">{{ title }}</div>
        <div class="value">{{ value }}</div>
      </div>
    </div>
  </div>
</template>
<script>
export default {
  props: {
    title: {
      type: String,
      default: '',
    },
    value: {
      type: Number,
      default: 0,
    },
    isRow: {
      type: Boolean,
      default: true,
    },
    background: {
      type: String,
      default: '',
    },
  },
}
</script>
<style lang="scss" scoped>
.card {
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid #ebeef5;
  width: 220px;
  height: 44px;
  border-radius: 2px;
  margin: 0px 10px 20px 0;
  background: #f0f3f8;
  .flex {
    display: flex;
    align-items: center;
    justify-content: center;
    flex-direction: row;
    // width: 90px;
    .title-1 {
      font-size: 14px;
      color: #666666;
      font-weight: 400;
      margin-right: 5px;
    }
    .value {
      font-size: 16px;
      font-weight: bold;
      color: #666666;
    }
  }
}
</style>
