<template>
  <div>
    <el-row>
      <el-col :span="12"
        ><h1 class="title-1 color3">{{ bucket }}</h1></el-col
      >
      <el-col :span="12" class="txt-r color6"
        ><el-button class="border-e5 font14" plain size="medium" @click="goBack"
          >返回列表</el-button
        ></el-col
      >
    </el-row>
    <div class="bucket_msg color9">
      <slot></slot>
    </div>
  </div>
</template>
<script>
export default {
  props: {
    bucket: {
      type: String,
      default: '',
      required: true,
    },
    goBack: {
      type: Function,
      default: () => ({}),
      required: true,
    },
  },
}
</script>
<style lang="scss" scoped>
.title-1 {
  font-weight: bold;
  margin-top: 8px;
  margin-bottom: 16px;
  font-size: 24px;
}
.border-e5 {
  border: 1px solid #e5e5e5;
}
.bucket_msg {
  margin-bottom: 40px;
}
.txt-r {
  text-align: right;
}
</style>
