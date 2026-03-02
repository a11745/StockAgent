<script setup lang="ts">
import { ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import { useMarketStore } from '@/stores/market'
import { useTask } from '@/hooks'
import type { StockBasic } from '@/api'

const router = useRouter()
const marketStore = useMarketStore()
const { analyzeStock, queryAnalysis } = useTask()

// 搜索关键词
const keyword = ref('')

// 搜索结果
const searchResults = ref<StockBasic[]>([])

// 加载状态
const loading = ref(false)

// 显示下拉
const showDropdown = ref(false)

// 防抖搜索
const debouncedSearch = useDebounceFn(async (value: string) => {
  if (!value.trim()) {
    searchResults.value = []
    return
  }
  
  loading.value = true
  try {
    searchResults.value = await marketStore.searchStocks(value)
  } finally {
    loading.value = false
  }
}, 300)

// 监听关键词变化
watch(keyword, (value) => {
  debouncedSearch(value)
})

// 选择股票
function selectStock(stock: StockBasic) {
  router.push(`/stock/${stock.ts_code}`)
  keyword.value = ''
  searchResults.value = []
  showDropdown.value = false
}

// 快速分析
async function quickAnalyze(stock: StockBasic, event: Event) {
  event.stopPropagation()
  
  const taskId = await analyzeStock(stock.ts_code)
  if (taskId) {
    router.push(`/analysis/${taskId}`)
  }
  
  keyword.value = ''
  searchResults.value = []
  showDropdown.value = false
}

// 失去焦点处理
function handleBlur() {
  setTimeout(() => {
    showDropdown.value = false
  }, 200)
}

// 自然语言查询
async function handleQuery() {
  if (!keyword.value.trim()) return
  
  // 检查是否是股票代码
  const isCode = /^[0-9]{6}$/.test(keyword.value) || /^[0-9]{6}\.[A-Z]{2}$/.test(keyword.value)
  
  if (isCode) {
    // 跳转到股票页
    const code = keyword.value.includes('.') ? keyword.value : `${keyword.value}.SZ`
    router.push(`/stock/${code}`)
  } else {
    // 自然语言查询
    const taskId = await queryAnalysis(keyword.value)
    if (taskId) {
      router.push(`/analysis/${taskId}`)
    }
  }
  
  keyword.value = ''
  showDropdown.value = false
}
</script>

<template>
  <div class="search-bar">
    <el-popover
      :visible="showDropdown && (searchResults.length > 0 || keyword.length > 0)"
      placement="bottom-start"
      :width="400"
      :show-arrow="false"
      :offset="4"
    >
      <template #reference>
        <el-input
          v-model="keyword"
          placeholder="搜索股票代码/名称，或输入问题..."
          :prefix-icon="Search"
          clearable
          @focus="showDropdown = true"
          @blur="handleBlur"
          @keyup.enter="handleQuery"
        >
          <template #suffix>
            <el-button
              v-if="keyword && !keyword.match(/^[0-9]/)"
              type="primary"
              size="small"
              link
              @click="handleQuery"
            >
              提问
            </el-button>
          </template>
        </el-input>
      </template>
      
      <!-- 搜索结果 -->
      <div class="search-dropdown">
        <div v-if="loading" class="loading-tip">
          <el-icon class="is-loading"><Loading /></el-icon>
          搜索中...
        </div>
        
        <template v-else-if="searchResults.length > 0">
          <div class="result-header">股票 ({{ searchResults.length }})</div>
          <div
            v-for="stock in searchResults.slice(0, 8)"
            :key="stock.ts_code"
            class="result-item"
            @click="selectStock(stock)"
          >
            <div class="stock-info">
              <span class="stock-name">{{ stock.name }}</span>
              <span class="stock-code">{{ stock.ts_code }}</span>
            </div>
            <div class="stock-meta">
              <span class="industry">{{ stock.industry }}</span>
              <el-button
                type="primary"
                size="small"
                link
                @click="quickAnalyze(stock, $event)"
              >
                <el-icon><DataAnalysis /></el-icon>
                分析
              </el-button>
            </div>
          </div>
        </template>
        
        <div v-else-if="keyword" class="empty-tip">
          <p>未找到相关股票</p>
          <p class="hint">按 Enter 发送问题给 AI 分析</p>
        </div>
      </div>
    </el-popover>
  </div>
</template>

<script lang="ts">
import { Search, Loading } from '@element-plus/icons-vue'
export default {
  data() {
    return { Search }
  }
}
</script>

<style lang="scss" scoped>
.search-bar {
  width: 100%;
  
  :deep(.el-input) {
    --el-input-bg-color: var(--bg-secondary);
    --el-input-border-color: transparent;
    
    .el-input__wrapper {
      border-radius: 10px;
      padding: 4px 12px;
      
      &:hover,
      &.is-focus {
        box-shadow: 0 0 0 1px var(--color-primary);
      }
    }
  }
}

.search-dropdown {
  max-height: 400px;
  overflow-y: auto;
  margin: -12px;
  
  .loading-tip,
  .empty-tip {
    padding: 24px;
    text-align: center;
    color: var(--text-secondary);
    
    .hint {
      font-size: 12px;
      margin-top: 8px;
      color: var(--text-tertiary);
    }
  }
  
  .result-header {
    padding: 8px 16px;
    font-size: 12px;
    color: var(--text-tertiary);
    background: var(--bg-secondary);
  }
  
  .result-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    cursor: pointer;
    
    &:hover {
      background: var(--bg-secondary);
    }
    
    .stock-info {
      display: flex;
      align-items: center;
      gap: 12px;
      
      .stock-name {
        font-weight: 500;
        color: var(--text-primary);
      }
      
      .stock-code {
        font-size: 12px;
        color: var(--text-tertiary);
        font-family: var(--font-mono);
      }
    }
    
    .stock-meta {
      display: flex;
      align-items: center;
      gap: 12px;
      
      .industry {
        font-size: 12px;
        color: var(--text-secondary);
        padding: 2px 8px;
        background: var(--bg-tertiary);
        border-radius: 4px;
      }
    }
  }
}
</style>
