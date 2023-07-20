
<template>
  <div class="o-dropdown">
    <el-popover
      v-model="visible"
      :width="internalWidth"
      :visible-arrow="visibleArrow"
      :popper-class="poperClass + 'poper o-dropdown-popper'"
      :trigger="trigger"
      :options="{
        placement: 'bottom'
      }"
    >
      <div class="o-dropdwon-popper-content" @click="hideContent">
        <slot name="list"></slot>
      </div>
      <el-button slot="reference" :type="type" class="o-dropdwon-popper-header">
        <slot name="header"></slot>
        <i :class="[visible ? 'active' : '', 'arrow-down']"></i>
      </el-button>
    </el-popover>
  </div>
</template>
<script>
export default {
  name: 'ODropdown',
  props: {
    type: {
      type: String,
      default: 'text'
    },
    poperClass: {
      type: String,
      default: ''
    },
    width: {
      type: [Number, String],
      default: 'auto'
    },
    trigger: {
      type: String,
      default: 'click'
    },
    visibleArrow: {
      type: Boolean,
      default: true
    }
  },
  data: function() {
    return {
      internalWidth: this.type !== 'text' ? 110 : this.width,
      visible: false
    }
  },
  methods: {
    hideContent(e) {
      this.visible = false
    }
  }
}
</script>
<style lang="scss">
.o-dropdown-arrow-button {
  width: 110px;
}
.el-popover {
  &.o-dropdown-popper {
    min-width: 110px;
    padding: 0;
    margin-top: 8px;
    margin-bottom: 8px;
    .o-dropdwon-popper-content {
      color: #606266;
      margin-top: 8px;
      margin-bottom: 8px;
      .is-disabled {
        cursor: default;
        color: #bbb;
        pointer-events: none;
      }
      .o-icon {
        margin-right: 5px;
      }
      .o-dropdown-item,
      li {
        position: relative;
        cursor: pointer;
        padding: 8px 12px;
        &:hover {
          color: $theme-color;
          border-color: #c1ede1;
          background-color: #eaf9f5;
        }
        &:visited {
          background-color: $theme-color;
          color: #ffffff;
        }
      }
    }
  }
}
</style>
<style lang="scss" scoped>
.o-dropdown {
  display: inline-block;
  vertical-align: top;
  .o-dropdwon-popper-header {
    width: 100%;
  }
}
.table-operate {
  padding: 0;
  color: $theme-color;
  border: 0;
  background: none;
  display: inline-block;
  margin: 0 6px 0 6px;
  .o-dropdwon-popper-header {
    margin-left: 0 !important;
  }
}
.o-dropdown-arrow-text,
.table-operate {
  position: relative;
  .arrow-down {
    &::after {
      position: absolute;
      right: -8px;
      top: 50%;
      margin: -5px 0 0 0;
      transition: transform 0.3s;
      content: '';
      height: 6px;
      width: 6px;
      border-width: 0 1px 1px 0;
      border-color: $theme-color;
      border-style: solid;
      transform: matrix(0.71, 0.71, -0.71, 0.71, 0, 0);
    }
  }
  .arrow-down.active::after {
    right: -8px;
    top: 50%;
    margin: -2px 0 0 0;
    transform-origin: center;
    transform: rotate(-135deg);
    transition: transform 0.3s;
  }
}
.o-dropdown-arrow-button {
  .arrow-down {
    &::after {
      position: absolute;
      right: 16px;
      top: 50%;
      margin: -3px 0 0 0;
      content: '';
      width: 0;
      height: 0;
      border-style: solid;
      border-width: 5px 4px 0 4px;
      transition: transform 0.3s;
      border-color: #fff transparent transparent transparent;
    }
  }
  .arrow-down.active::after {
    transform-origin: center;
    transform: rotate(-180deg);
    transition: transform 0.3s;
  }

  .o-dropdwon-popper-header {
    position: relative;
    display: inline-block;
    vertical-align: middle;
    text-align: left;
    margin-right: 4px;
    &::before {
      margin-right: 4px;
      font-family: element-icons !important;
      font-style: normal;
      font-weight: 400;
      font-variant: normal;
      text-transform: none;
      vertical-align: baseline;
      display: inline-block;
      -webkit-font-smoothing: antialiased;
      content: '\e798';
    }
  }
}
</style>
