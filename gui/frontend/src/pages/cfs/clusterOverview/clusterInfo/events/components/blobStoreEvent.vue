<template>
  <div>
    <el-form>
      <EbsClusterSelectForm v-model="clusterId" :clearable="false" @input="onClusterChange"></EbsClusterSelectForm>
    </el-form>
    <el-collapse v-model="activeName">
      <el-collapse-item name="balance">
        <template slot="title">
          <div class="flex-title">
            <div class="color3">
              <span>任务:</span>
              <span class="color9">balance</span>
            </div>
            <div>
              <el-switch
                v-if="!!balance.status"
                :value="balance.status"
                inactive-text="关 "
                active-text="开 "
                active-value="true"
                inactive-value="false"
                active-color="#00c9c9"
                :width="45"
                @click.native.stop
                @change="onChangeValue($event, 'balance')"
              ></el-switch>
            </div>
          </div>
        </template>
        <u-table :data="balance.list" is-need-client-paging border>
          <el-table-column prop="finishing_cnt" label="finishing_cnt" width="110" fixed="left"></el-table-column>
          <el-table-column prop="preparing_cnt" label="preparing_cnt" width="130" fixed="left"></el-table-column>
          <el-table-column prop="worker_doing_cnt" label="worker_doing_cnt" width="150" fixed="left"></el-table-column>
          <el-table-column prop="shard_cnt" label="最近20分钟任务情况" align="center" width="1800">
            <template slot-scope="scope">
              <el-row v-if="scope.row.finished_cnt.length || scope.row.shard_cnt.length" style="display: flex">
                <div style="width :120px">序号</div>
                <div
                  v-for="(item, index) in 20"
                  :key="index"
                  style="width :6%"
                >{{ 20 - index }}</div>
              </el-row>
              <el-row v-if="scope.row.finished_cnt.length" style="display: flex">
                <div style="width :120px">每分钟完成数</div>
                <div
                  v-for="(item, index) in scope.row.finished_cnt"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
              <el-row v-if="scope.row.shard_cnt.length" style="display: flex">
                <div style="width :120px">shard_cnt</div>
                <div
                  v-for="(item, index) in scope.row.shard_cnt"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
            </template>
          </el-table-column>
        </u-table>
      </el-collapse-item>
    </el-collapse>
    <el-collapse>
      <el-collapse-item name="blob_delete">
        <template slot="title">
          <div class="flex-title">
            <div class="color3">
              <span>任务:</span>
              <span class="color9">blob_delete</span>
            </div>
            <div>
              <el-switch
                v-if="!!blob_delete.status"
                :value="blob_delete.status"
                inactive-text="关"
                active-text="开"
                active-value="true"
                inactive-value="false"
                active-color="#00c9c9"
                :width="45"
                @click.native.stop
                @change="onChangeValue($event, 'blob_delete')"
              ></el-switch>
            </div>
          </div>
        </template>
        <u-table :data="blob_delete.list" is-need-client-paging border>
          <el-table-column prop="host_name" label="host_name" width="100" fixed="left"></el-table-column>
          <el-table-column prop="host" label="host" width="100" fixed="left">
            <template slot-scope="scope">
              {{ `${(scope.row.host || '').replace('down', '宕机')}` }}
            </template>
          </el-table-column>
          <el-table-column prop="shard_cnt" label="最近20分钟任务情况" align="center" width="1800">
            <template slot-scope="scope">
              <el-row v-if="scope.row.success_per_min.length || scope.row.failed_per_min.length" style="display: flex">
                <div style="width :120px">序号</div>
                <div
                  v-for="(item, index) in 20"
                  :key="index"
                  style="width :6%"
                >{{ 20 - index }}</div>
              </el-row>
              <el-row v-if="scope.row.success_per_min.length" style="display: flex">
                <div style="width :120px">每分钟完成数</div>
                <div
                  v-for="(item, index) in scope.row.success_per_min"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
              <el-row v-if="scope.row.failed_per_min.length" style="display: flex">
                <div style="width :120px">每分钟失败数</div>
                <div
                  v-for="(item, index) in scope.row.failed_per_min"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
            </template>
          </el-table-column>
        </u-table>
      </el-collapse-item>
    </el-collapse>
    <el-collapse>
      <el-collapse-item name="disk_drop">
        <template slot="title">
          <div class="flex-title">
            <div class="color3">
              <span>任务:</span>
              <span class="color9">disk_drop</span>
            </div>
            <div>
              <el-switch
                v-if="!!disk_drop.status"
                :value="disk_drop.status"
                inactive-text="关"
                active-text="开"
                active-value="true"
                inactive-value="false"
                active-color="#00c9c9"
                :width="45"
                @click.native.stop
                @change="onChangeValue($event, 'disk_drop')"
              ></el-switch>
            </div>
          </div>
        </template>
        <u-table :data="disk_drop.list" is-need-client-paging border>
          <el-table-column prop="dropping_disk_id" label="dropping_disk_id" width="140" fixed="left"></el-table-column>
          <el-table-column prop="dropped_tasks_cnt" label="dropped_tasks_cnt" width="150" fixed="left"></el-table-column>
          <el-table-column prop="finishing_cnt" label="finishing_cnt" width="120" fixed="left"></el-table-column>
          <el-table-column prop="preparing_cnt" label="preparing_cnt" width="120" fixed="left"></el-table-column>
          <el-table-column prop="total_tasks_cnt" label="total_tasks_cnt" width="120" fixed="left"></el-table-column>
          <el-table-column prop="worker_doing_cnt" label="worker_doing_cnt" width="140" fixed="left"></el-table-column>
          <el-table-column prop="shard_cnt" label="最近20分钟任务情况" align="center" width="1800">
            <template slot-scope="scope">
              <el-row v-if="scope.row.finished_cnt.length || scope.row.shard_cnt.length" style="display: flex">
                <div style="width :120px">序号</div>
                <div
                  v-for="(item, index) in 20"
                  :key="index"
                  style="width :6%"
                >{{ 20 - index }}</div>
              </el-row>
              <el-row v-if="scope.row.finished_cnt.length" style="display: flex">
                <div style="width :120px">每分钟完成数</div>
                <div
                  v-for="(item, index) in scope.row.finished_cnt"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
              <el-row v-if="scope.row.shard_cnt.length" style="display: flex">
                <div style="width :120px">shard_cnt</div>
                <div
                  v-for="(item, index) in scope.row.shard_cnt"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
            </template>
          </el-table-column>
        </u-table>
      </el-collapse-item>
    </el-collapse>
    <el-collapse>
      <el-collapse-item name="disk_repair">
        <template slot="title">
          <div class="flex-title">
            <div class="color3">
              <span>任务:</span>
              <span class="color9">disk_repair</span>
            </div>
            <div>
              <el-switch
                v-if="disk_repair.status"
                :value="disk_repair.status"
                inactive-text="关"
                active-text="开"
                active-value="true"
                inactive-value="false"
                active-color="#00c9c9"
                :width="45"
                @click.native.stop
                @change="onChangeValue($event, 'disk_repair')"
              ></el-switch>
            </div>
          </div>
        </template>
        <u-table :data="disk_repair.list" is-need-client-paging border>
          <el-table-column prop="repairing_disk_id" label="repairing_disk_id" width="135" fixed="left"></el-table-column>
          <el-table-column prop="repaired_tasks_cnt" label="repaired_tasks_cnt" width="150" fixed="left"></el-table-column>
          <el-table-column prop="finishing_cnt" label="finishing_cnt" width="120" fixed="left"></el-table-column>
          <el-table-column prop="preparing_cnt" label="preparing_cnt" width="120" fixed="left"></el-table-column>
          <el-table-column prop="total_tasks_cnt" label="total_tasks_cnt" width="120" fixed="left"></el-table-column>
          <el-table-column prop="worker_doing_cnt" label="worker_doing_cnt" width="140" fixed="left"></el-table-column>
          <el-table-column prop="shard_cnt" label="最近20分钟任务情况" align="center" width="1800">
            <template slot-scope="scope">
              <el-row v-if="scope.row.finished_cnt.length || scope.row.shard_cnt.length" style="display: flex">
                <div style="width :120px">序号</div>
                <div
                  v-for="(item, index) in 20"
                  :key="index"
                  style="width :6%"
                >{{ 20 - index }}</div>
              </el-row>
              <el-row v-if="scope.row.finished_cnt.length" style="display: flex">
                <div style="width :120px">每分钟完成数</div>
                <div
                  v-for="(item, index) in scope.row.finished_cnt"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
              <el-row v-if="scope.row.shard_cnt.length" style="display: flex">
                <div style="width :120px">shard_cnt</div>
                <div
                  v-for="(item, index) in scope.row.shard_cnt"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
            </template>
          </el-table-column>
        </u-table>
      </el-collapse-item>
    </el-collapse>
    <el-collapse>
      <el-collapse-item name="shard_repair">
        <template slot="title">
          <div class="flex-title">
            <div class="color3">
              <span>任务:</span>
              <span class="color9">shard_repair</span>
            </div>
            <div>
              <el-switch
                v-if="shard_repair.status"
                :value="shard_repair.status"
                active-text="开"
                inactive-text="关"
                active-value="true"
                inactive-value="false"
                active-color="#00c9c9"
                :width="45"
                @click.native.stop
                @change="onChangeValue($event, 'shard_repair')"
              ></el-switch>
            </div>
          </div>
        </template>
        <u-table :data="shard_repair.list" is-need-client-paging border>
          <el-table-column prop="host_name" label="host_name" width="100" fixed="left"></el-table-column>
          <el-table-column prop="host" label="host" width="200" fixed="left">
            <template slot-scope="scope">
              {{ `${(scope.row.host || '').replace('down', '宕机')}` }}
            </template>
          </el-table-column>
          <el-table-column prop="shard_cnt" label="最近20分钟任务情况" align="center" width="1800">
            <template slot-scope="scope">
              <el-row v-if="scope.row.success_per_min.length || scope.row.failed_per_min.length" style="display: flex">
                <div style="width :120px">序号</div>
                <div
                  v-for="(item, index) in 20"
                  :key="index"
                  style="width :6%"
                >{{ 20 - index }}</div>
              </el-row>
              <el-row v-if="scope.row.success_per_min.length" style="display: flex">
                <div style="width :120px">每分钟完成数</div>
                <div
                  v-for="(item, index) in scope.row.success_per_min"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
              <el-row v-if="scope.row.failed_per_min.length" style="display: flex">
                <div style="width :120px">每分钟失败数</div>
                <div
                  v-for="(item, index) in scope.row.failed_per_min"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
            </template>
          </el-table-column>
        </u-table>
      </el-collapse-item>
    </el-collapse>
    <el-collapse>
      <el-collapse-item name="volume_inspect">
        <template slot="title">
          <div class="flex-title">
            <div class="color3">
              <span>任务:</span>
              <span class="color9">volume_inspect</span>
            </div>
            <div>
              <el-switch
                v-if="volume_inspect.status"
                :value="volume_inspect.status"
                inactive-text="关"
                active-text="开"
                active-value="true"
                inactive-value="false"
                active-color="#00c9c9"
                :width="45"
                @click.native.stop
                @change="onChangeValue($event, 'volume_inspect')"
              ></el-switch>
            </div>
          </div>
        </template>
        <u-table :data="volume_inspect.list" is-need-client-paging>
          <el-table-column prop="shard_cnt" label="最近20分钟任务情况" align="center" width="1800">
            <template slot-scope="scope">
              <el-row v-if="scope.row.finished_per_min.length || scope.row.time_out_per_min.length" style="display: flex">
                <div style="width :120px">序号</div>
                <div
                  v-for="(item, index) in 20"
                  :key="index"
                  style="width :6%"
                >{{ 20 - index }}</div>
              </el-row>
              <el-row v-if="scope.row.finished_per_min.length" style="display: flex">
                <div style="width :120px">每分钟完成数</div>
                <div
                  v-for="(item, index) in scope.row.finished_per_min"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
              <el-row v-if="scope.row.time_out_per_min.length" style="display: flex">
                <div style="width :120px">每分钟超时数</div>
                <div
                  v-for="(item, index) in scope.row.time_out_per_min"
                  :key="index"
                  style="width :6%"
                >{{ item }}</div>
              </el-row>
            </template>
          </el-table-column>
        </u-table>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>
<script>
import EbsClusterSelectForm from '@/components/EbsClusterSelectForm.vue'
import { getBackTaskList, setBackTaskStatus } from '@/api/ebs/ebs'
import UTable from '@/components/uPageTable.vue'
import { dealStrArray } from '@/utils'
export default {
  components: {
    EbsClusterSelectForm,
    UTable,
  },
  data() {
    return {
      activeName: '',
      clusterId: '',
      ...this.resetData(),
    }
  },
  computed: {
    clusterInfo() {
      return JSON.parse(sessionStorage.getItem('clusterInfo'))
    },
  },
  mounted() {
    this.getForms()
  },
  methods: {
    resetData(flag = false) {
      const resetItems = ['balance', 'blob_delete', 'disk_drop', 'disk_repair', 'shard_repair', 'volume_inspect']
      const result = flag ? this : {}
      resetItems.forEach(item => {
        result[item] = {
          status: '',
          list: [],
        }
      })
      return result
    },
    onChangeValue(value, k) {
      this.changeOnOff(value, k)
    },
    getForms() {
      const { clusterName } = this.clusterInfo
      const forms = { region: clusterName, clusterId: this.clusterId }
      this.forms = forms
      this.getData()
    },
    onClusterChange() {
      this.getForms()
    },
    async getData() {
      this.resetData(true)
      try {
        const { data: res } = await getBackTaskList(this.forms)
        // eslint-disable-next-line camelcase
        const { balance, blob_delete, drop, inspect, repair, shard_repair } = res || {}
        // balance
        this.balance.status = ((balance || [])?.[0]?.enable ?? '') + ''
        this.balance.list = (balance || []).map(item => {
          return {
            ...item,
            finished_cnt: dealStrArray(item?.stats_per_min?.finished_cnt || ''),
            shard_cnt: dealStrArray(item?.stats_per_min?.shard_cnt || ''),
          }
        })
        // blob_delete
        // eslint-disable-next-line camelcase
        this.blob_delete.status = ((blob_delete || [])?.[0]?.blob_delete?.enable ?? '') + ''

        // eslint-disable-next-line camelcase
        this.blob_delete.list = (blob_delete || []).map(item => {
          return {
            ...item,
            failed_per_min: dealStrArray(item?.blob_delete?.failed_per_min || ''),
            success_per_min: dealStrArray(item?.blob_delete?.success_per_min || ''),
          }
        })
        // disk_drop
        this.disk_drop.status = ((drop || [])?.[0]?.enable ?? '') + ''
        this.disk_drop.list = (drop || []).map(item => {
          return {
            ...item,
            finished_cnt: dealStrArray(item?.stats_per_min?.finished_cnt || ''),
            shard_cnt: dealStrArray(item?.stats_per_min?.shard_cnt || ''),
          }
        })
        // disk_repair
        this.disk_repair.status = ((repair || [])?.[0]?.enable ?? '') + ''
        this.disk_repair.list = (repair || []).map(item => {
          return {
            ...item,
            finished_cnt: dealStrArray(item?.stats_per_min?.finished_cnt || ''),
            shard_cnt: dealStrArray(item?.stats_per_min?.shard_cnt || ''),
          }
        })
        // shard_repair
        // eslint-disable-next-line camelcase
        this.shard_repair.status = ((shard_repair || [])?.[0]?.shard_repair?.enable ?? '') + ''
        // eslint-disable-next-line camelcase
        this.shard_repair.list = (shard_repair || []).map(item => {
          return {
            ...item,
            failed_per_min: dealStrArray(item?.shard_repair?.failed_per_min || ''),
            success_per_min: dealStrArray(item?.shard_repair?.success_per_min || ''),
          }
        })
        // volume_inspect
        this.volume_inspect.status = ((inspect || [])?.[0]?.enable ?? '') + ''
        this.volume_inspect.list = (inspect || []).map(item => {
          return {
            finished_per_min: dealStrArray(item?.finished_per_min || ''),
            time_out_per_min: dealStrArray(item?.time_out_per_min || ''),
          }
        })
      } catch {
        this.resetData(true)
      }
    },
    async changeOnOff(v, k) {
      const { region, clusterId } = this.forms
      try {
        await this.$confirm(
          `请确认是否进行${v === 'true' ? '开启' : '关闭'}操作`,
          '提示',
          {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            type: 'warning',
          },
        )
        await setBackTaskStatus({
          region,
          clusterId,
          key: k,
          value: v,
        })
        this[k].status = v
        this.$message.success('操作成功')
        setTimeout(() => {
          this.getForms(this.forms)
        }, 60 * 1000)
      } catch (e) {}
    },
  },
}
</script>
<style lang="scss" scoped>
.width40 {
  width: 45px;
}
.p-t-26 {
  padding-top: 26px;
}
.noda {
  display: flex;
  height: 300px;
  text-align: center;
  align-items: center;
  justify-content: center;
}
.color3 {
  color: #333;
  margin-right: 10px;
}
.color9 {
  color: #999;
}
.flex-title {
  width: 240px;
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
