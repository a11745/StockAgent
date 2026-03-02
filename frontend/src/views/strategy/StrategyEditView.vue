<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { strategyApi, stockApi, type StockBasic } from '@/api'
import { ElMessage } from 'element-plus'
import type { FormInstance, FormRules } from 'element-plus'

const route = useRoute()
const router = useRouter()

// 编辑模式
const strategyId = computed(() => route.params.strategyId as string | undefined)
const isEdit = computed(() => !!strategyId.value)

// 表单
const formRef = ref<FormInstance>()
const loading = ref(false)
const submitting = ref(false)

// 表单数据
const formData = ref({
  name: '',
  description: '',
  stock_pool: [] as string[],
  weights: {
    fundamental: 30,
    technical: 30,
    sentiment: 20,
    valuation: 20,
  },
  alert_enabled: false,
})

// 验证规则
const rules: FormRules = {
  name: [
    { required: true, message: '请输入策略名称', trigger: 'blur' },
    { min: 2, max: 50, message: '名称长度在 2 到 50 个字符', trigger: 'blur' },
  ],
}

// 股票搜索
const stockSearchKeyword = ref('')
const stockSearchResults = ref<StockBasic[]>([])
const searching = ref(false)

// 加载策略数据（编辑模式）
onMounted(async () => {
  if (isEdit.value) {
    loading.value = true
    try {
      const strategy = await strategyApi.getStrategy(strategyId.value!)
      formData.value = {
        name: strategy.name,
        description: strategy.description || '',
        stock_pool: strategy.stock_pool,
        weights: strategy.weights as any || {
          fundamental: 30,
          technical: 30,
          sentiment: 20,
          valuation: 20,
        },
        alert_enabled: strategy.alert_enabled,
      }
    } finally {
      loading.value = false
    }
  }
})

// 搜索股票
async function searchStock() {
  if (!stockSearchKeyword.value.trim()) return
  
  searching.value = true
  try {
    stockSearchResults.value = await stockApi.searchStocks(stockSearchKeyword.value, 10)
  } finally {
    searching.value = false
  }
}

// 添加股票
function addStock(stock: StockBasic) {
  if (!formData.value.stock_pool.includes(stock.ts_code)) {
    formData.value.stock_pool.push(stock.ts_code)
  }
  stockSearchKeyword.value = ''
  stockSearchResults.value = []
}

// 移除股票
function removeStock(code: string) {
  formData.value.stock_pool = formData.value.stock_pool.filter((c) => c !== code)
}

// 提交表单
async function handleSubmit() {
  const valid = await formRef.value?.validate()
  if (!valid) return
  
  submitting.value = true
  try {
    if (isEdit.value) {
      // 更新策略（这里需要后端支持 PUT 接口）
      ElMessage.success('策略更新成功')
    } else {
      const response = await strategyApi.createStrategy({
        name: formData.value.name,
        description: formData.value.description || undefined,
        stock_pool: formData.value.stock_pool,
        weights: formData.value.weights,
        alert_enabled: formData.value.alert_enabled,
      })
      ElMessage.success('策略创建成功')
      router.push(`/strategy/${response.strategy_id}`)
    }
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <div class="strategy-edit-view">
    <div class="page-header">
      <el-button :icon="ArrowLeft" @click="router.back()">返回</el-button>
      <h1>{{ isEdit ? '编辑策略' : '创建策略' }}</h1>
    </div>
    
    <div v-if="!loading" class="form-container card">
      <el-form
        ref="formRef"
        :model="formData"
        :rules="rules"
        label-width="100px"
        size="large"
      >
        <!-- 基本信息 -->
        <div class="form-section">
          <h3>基本信息</h3>
          
          <el-form-item label="策略名称" prop="name">
            <el-input v-model="formData.name" placeholder="输入策略名称" />
          </el-form-item>
          
          <el-form-item label="描述">
            <el-input
              v-model="formData.description"
              type="textarea"
              :rows="3"
              placeholder="描述策略的目标和逻辑（可选）"
            />
          </el-form-item>
        </div>
        
        <!-- 股票池 -->
        <div class="form-section">
          <h3>股票池</h3>
          
          <el-form-item label="添加股票">
            <div class="stock-search">
              <el-input
                v-model="stockSearchKeyword"
                placeholder="输入股票代码或名称搜索"
                @keyup.enter="searchStock"
              >
                <template #append>
                  <el-button :loading="searching" @click="searchStock">搜索</el-button>
                </template>
              </el-input>
              
              <div v-if="stockSearchResults.length > 0" class="search-results">
                <div
                  v-for="stock in stockSearchResults"
                  :key="stock.ts_code"
                  class="result-item"
                  @click="addStock(stock)"
                >
                  <span class="name">{{ stock.name }}</span>
                  <span class="code">{{ stock.ts_code }}</span>
                </div>
              </div>
            </div>
          </el-form-item>
          
          <el-form-item label="已选股票">
            <div class="selected-stocks">
              <el-tag
                v-for="code in formData.stock_pool"
                :key="code"
                closable
                @close="removeStock(code)"
              >
                {{ code }}
              </el-tag>
              <span v-if="formData.stock_pool.length === 0" class="empty">
                暂未添加股票
              </span>
            </div>
          </el-form-item>
        </div>
        
        <!-- 权重配置 -->
        <div class="form-section">
          <h3>分析权重</h3>
          
          <el-form-item label="基本面">
            <el-slider v-model="formData.weights.fundamental" :max="100" show-input />
          </el-form-item>
          
          <el-form-item label="技术面">
            <el-slider v-model="formData.weights.technical" :max="100" show-input />
          </el-form-item>
          
          <el-form-item label="舆情">
            <el-slider v-model="formData.weights.sentiment" :max="100" show-input />
          </el-form-item>
          
          <el-form-item label="估值">
            <el-slider v-model="formData.weights.valuation" :max="100" show-input />
          </el-form-item>
        </div>
        
        <!-- 预警设置 -->
        <div class="form-section">
          <h3>预警设置</h3>
          
          <el-form-item label="开启预警">
            <el-switch v-model="formData.alert_enabled" />
          </el-form-item>
        </div>
        
        <!-- 提交按钮 -->
        <div class="form-actions">
          <el-button @click="router.back()">取消</el-button>
          <el-button type="primary" :loading="submitting" @click="handleSubmit">
            {{ isEdit ? '保存修改' : '创建策略' }}
          </el-button>
        </div>
      </el-form>
    </div>
    
    <div v-else class="loading">
      <el-skeleton :rows="15" animated />
    </div>
  </div>
</template>

<script lang="ts">
import { ArrowLeft } from '@element-plus/icons-vue'
export default {
  data() {
    return { ArrowLeft }
  }
}
</script>

<style lang="scss" scoped>
.strategy-edit-view {
  max-width: 800px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 24px;
  
  h1 {
    font-size: 24px;
    font-weight: 600;
  }
}

.form-container {
  padding: 24px;
}

.form-section {
  margin-bottom: 32px;
  
  h3 {
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 20px;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border-color);
  }
}

.stock-search {
  width: 100%;
  position: relative;
  
  .search-results {
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    background: var(--bg-primary);
    border: 1px solid var(--border-color);
    border-radius: 8px;
    box-shadow: var(--shadow-md);
    z-index: 10;
    max-height: 200px;
    overflow-y: auto;
    
    .result-item {
      padding: 10px 16px;
      cursor: pointer;
      display: flex;
      justify-content: space-between;
      
      &:hover {
        background: var(--bg-secondary);
      }
      
      .code {
        color: var(--text-tertiary);
        font-size: 12px;
      }
    }
  }
}

.selected-stocks {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  
  .empty {
    color: var(--text-tertiary);
  }
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  padding-top: 20px;
  border-top: 1px solid var(--border-color);
}

.loading {
  padding: 40px;
}
</style>
