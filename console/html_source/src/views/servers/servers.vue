<template>
  <div class="cluster servers">
    <div class="server-title" v-if="summary">
      <span>{{$t('chubaoFS.servers.Cluster')}}: {{summary.name}}<b>{{$t('chubaoFS.servers.TotalVolume')}} : {{summary.volumeCount}}</b></span>
      <el-button type="primary" class="ml50" @click="goServerList">{{$t('chubaoFS.servers.ServerList')}}</el-button>
    </div>
    
    <div class="server-tree">
      <div class="text-center">
        <vue2-org-tree
          name="test"
          :data="data"
          :horizontal="horizontal"
          :collapsable="collapsable"
          :label-class-name="labelClassName"
          :render-content="renderContent"
          @on-expand="onExpand"
          @on-node-click="onNodeClick"
        />
      </div>
      <div class="photo"></div>
    </div>
  </div>
</template>

<script>
import baseGql from '../../graphql/server'
export default {
  name: 'Server',
  data () {
    return {
      summary: null,
      data: {
        id: 0,
        label: 'Cluster_Baud Storage',
        children: [
          {
            id: 1,
            label: 'Master',
            children: []
          },
          {
            id: 2,
            label: 'MataNode',
            children: []
          },
          {
            id: 3,
            label: 'DataNode',
            children: []
          }
        ]
      },
      dataPort: 9500,
      metaPort: 9500,
      masterPort: 9500,
      horizontal: true,
      collapsable: true,
      expandAll: true,
      labelClassName: 'bg-white'
    }
  },
  methods: {
    setNum () {
      const itemData = document.getElementsByClassName('org-tree-node-label-inner bg-white')
      // console.log(itemData)
      for (let i = 0; i < itemData.length; i++) {
        let indexNum = null
        switch (itemData[i].innerText) {
          case 'Master':
            indexNum = 0
            break
          case 'MataNode':
            indexNum = 1
            break
          case 'DataNode':
            indexNum = 2
            break
          default:
        }
        if (indexNum !== null) {
          const num = this.data.children[indexNum].children.length
          const insertElement = document.createElement('i')
          insertElement.classList.add('server-num')
          insertElement.innerText = num
          itemData[i].appendChild(insertElement)
        }
      }
    },
    renderContent (h, data) {
      const that = this
      setTimeout(function () {
        that.setNum()
      }, 20)
      return data.label
    },
    onExpand (e, data) {
      if ('expand' in data) {
        data.expand = !data.expand
        if (!data.expand && data.children) {
          this.collapse(data.children)
        }
      } else {
        this.$set(data, 'expand', true)
      }
    },
    // 点击选项执行的方法，可以用于跳转到其他链接，注意一定要写协议头
    onNodeClick (e, data) {
      if (data.url == null) {
        return false
      } else {
        window.open(data.url)
      }
    },
    collapse (list) {
      var _this = this
      list.forEach(function (child) {
        if (child.expand) {
          child.expand = false
        }
        child.children && _this.collapse(child.children)
      })
    },
    expandChange () {
      this.toggleExpand(this.data, this.expandAll)
    },
    toggleExpand (data, val) {
      var _this = this
      if (Array.isArray(data)) {
        data.forEach(function (item) {
          _this.$set(item, 'expand', val)
          if (item.children) {
            _this.toggleExpand(item.children, val)
          }
        })
      } else {
        this.$set(data, 'expand', val)
        if (data.children) {
          _this.toggleExpand(data.children, val)
        }
      }
    },
    goServerList () {
      this.$router.push({
        name: 'serverList'
      })
    },
    dataProcessing (data) {
      let newDataArr = []
      for (let eachItem of data) {
        const obj = {
          id: eachItem,
          label: eachItem.addr ? eachItem.addr : eachItem
        }
        newDataArr.push(obj)
      }
      return newDataArr
    },
    queryExpandAll () {
      const that = this
      const variables = {
        num: 10000
      }
      this.apollo.query(this.url.cluster, baseGql.queryExpandAll, variables).then((res) => {
        if (res) {
          that.summary = res.data.clusterView
          that.data.children[0].children = this.dataProcessing(res.data.masterList)
          that.data.children[1].children = this.dataProcessing(res.data.metaNodeList)
          that.data.children[2].children = this.dataProcessing(res.data.dataNodeList)
        }
        this.expandChange()
        setTimeout(function () {
          that.setNum()
        }, 20)
      })
    }
  },
  mounted () {
    this.queryExpandAll()
  }
}
</script>

<style scoped>
  .server-title {
    width: 100%;
    padding-bottom: 18px;
    border-bottom: 1px solid #D8D8D8;
  }
  .server-title b {
    margin-left: 40px;
    font-weight: normal;
  }
  .server-tree {
    position: relative;
    min-height: 700px;
  }
  .server-tree .photo {
    position: absolute;
    right: 10px;
    bottom: 10px;
    width: 318px;
    height: 220px;
    background: url("../../assets/images/photo.png");
    background-size: 318px 220px;
  }
  .partition-right {
    display: inline-block;
    float: right;
  }
</style>
<style>
  .org-tree-node-label .org-tree-node-label-inner{
    /*position: relative;*/
  }
  .org-tree-node-label .org-tree-node-label-inner .server-num {
    width: 14px;
    height: 14px;
    background: rgba(245,166,35,1);
    border-radius: 14px;
    font-size: 12px;
    color: white;
    line-height: 14px;
    display: inline-block;
    text-align: center;
    margin-left: 5px;
    /*position: absolute;*/
    /*right: -5px;*/
    /*top: -5px;*/
  }
</style>
