<template>
  <div class="search">
    <el-row>
      <!-- <SearchRegionCom :forms.sync="forms" @finish="finish">
        <el-form-item label="卷：">
          <el-input
            v-model="forms.volumn"
            style="width: 180px"
            placeholder="请输入卷id"
            clearable
            :disabled="!!forms.status"
          >
          </el-input>
        </el-form-item>
        <el-form-item label="状态：">
          <el-select
            v-model="forms.status"
            style="width: 180px"
            placeholder="请选择"
            :disabled="!!forms.volumn"
            clearable
          >
            <el-option
              v-for="item in statusList"
              :key="item.value"
              :label="item.label"
              :value="item.value"
            ></el-option>
          </el-select>
        </el-form-item>
        <el-button
          type="primary"
          icon="el-icon-search"
          @click="searchClick"
        >搜索</el-button>
        <el-button type="plain" @click="reset">重置</el-button>
      </SearchRegionCom> -->
      <el-form :inline="true" :model="forms">
        <el-form-item label="卷：">
          <el-input
            v-model="forms.volumn"
            style="width: 180px"
            placeholder="请输入卷id"
            clearable
            :disabled="!!forms.status"
          >
          </el-input>
        </el-form-item>
        <el-form-item label="状态：">
          <el-select
            v-model="forms.status"
            style="width: 180px"
            placeholder="请选择"
            :disabled="!!forms.volumn"
            clearable
          >
            <el-option
              v-for="item in statusList"
              :key="item.value"
              :label="item.label"
              :value="item.value"
            ></el-option>
          </el-select>
        </el-form-item>
        <el-button
          type="primary"
          icon="el-icon-search"
          @click="searchClick"
        >搜索</el-button>
        <el-button type="plain" @click="reset">重置</el-button>
      </el-form>
    </el-row>
  </div>
</template>
<script>
// 三种情况来查询接口

import { volStatusList } from '@/pages/cfs/status.conf'
import { getVolList, getVolListById, getVolListByStatus } from '@/api/ebs/ebs'
export default {
  components: {
  },
  data() {
    return {
      forms: {
        volumn: '',
        status: '',
      },
    }
  },
  computed: {
    clusterInfo() {
      return JSON.parse(sessionStorage.getItem('clusterInfo'))
    },
  },
  created() {
    this.initData()
  },
  mounted() {

  },
  methods: {
    // finish(forms) {
    //   this.forms = {
    //     ...this.forms,
    //     ...forms,
    //   }
    //   this.$emit('finish', this.forms)
    // },
    initData() {
      this.statusList = volStatusList
    },
    searchClick(flag = true) {
      const { volumn, status } = this.forms
      const { name } = this.clusterInfo
      let fn = null
      if (!volumn && !status) {
        fn = getVolList({ cluster: name })
        fn && this.$emit('set-forms', fn, null, { cluster: name }, flag)
      } else if (volumn) {
        fn = getVolListById({ cluster: name })
        fn &&
          this.$emit('set-forms', fn, { vid: volumn }, { cluster: name }, flag)
      } else if (status) {
        fn = getVolListByStatus({ cluster: name })
        fn &&
          this.$emit(
            'set-forms',
            fn,
            { status: status },
            { cluster: name },
            flag,
          )
      }
    },
    reset() {
      this.forms.volumn = ''
      this.forms.status = ''
      this.$emit('reset')
    },
  },
}
</script>
<style lang="scss" scoped>
.input {
  width: 150px;
}
.search {
  position: relative;
  display: flex;
  flex-direction: row;
  align-items: center;
}
.radio-g {
  width: 660px;
  margin-left: -8px;
}
.search-btn {
  width: 200px;
}
.label {
  padding-top: 11px;
}
.status {
  margin-left: 10px;
}
.w-45 {
  width: 245px;
}
</style>
