<script setup lang="ts">
/**
 * 仪表盘页面 - 首页
 * 
 * 深度视觉升级版本 - 专业金融/AI助手风格
 */

import { onMounted, ref, computed } from 'vue'
import { ElMessage, ElAutocomplete } from 'element-plus'
import { Search, TrendCharts, DataAnalysis, Promotion} from '@element-plus/icons-vue'
import MarketOverviewCard from '@/components/dashboard/MarketOverviewCard.vue'
import WatchlistCard from '@/components/dashboard/WatchlistCard.vue'
import TaskProgressCard from '@/components/dashboard/TaskProgressCard.vue'
import RecentTasks from '@/components/dashboard/RecentTasks.vue'
import QuickActions from '@/components/dashboard/QuickActions.vue'
import { useTask, useWebSocket } from '@/hooks'
import { useMarketStore } from '@/stores'
import { stockApi } from '@/api'
import type { StockBasic } from '@/api/types'

// ==================== Hooks ====================

const { analyzeStock, analyzeMarket, queryAnalysis, isCreating } = useTask()
const { connect, isConnected } = useWebSocket()
const marketStore = useMarketStore()

// ==================== 状态 ====================

const searchQuery = ref('')
const stockCode = ref('')
const selectedStock = ref<StockBasic | null>(null)
const isSearchFocused = ref(false)

// ==================== 计算属性 ====================

const searchPlaceholder = computed(() => {
  const examples = [
    '帮我分析一下贵州茅台的投资价值',
    '今天大盘走势如何？',
    '比亚迪最近的财务状况怎么样',
    '分析一下新能源板块的投资机会',
  ]
  return examples[Math.floor(Date.now() / 10000) % examples.length]
})

// ==================== 方法 ====================

async function handleSearch(): Promise<void> {
  if (!searchQuery.value.trim()) {
    ElMessage.warning('请输入查询内容')
    return
  }
  
  await queryAnalysis(searchQuery.value)
  searchQuery.value = ''
}

/**
 * 股票搜索 - 远程获取建议列表
 */
function handleStockSearch(queryString: string, cb: (results: StockBasic[]) => void): void {
  if (!queryString.trim()) {
    cb([])
    return
  }
  
  stockApi.searchStocks(queryString, 10)
    .then(results => cb(results))
    .catch(error => {
      console.error('Stock search failed:', error)
      cb([])
    })
}

/**
 * 选中股票
 */
function handleStockSelect(stock: Record<string, any>): void {
  selectedStock.value = stock as StockBasic
  stockCode.value = stock.ts_code || ''
}

async function handleQuickAnalyze(): Promise<void> {
  if (!stockCode.value.trim()) {
    ElMessage.warning('请输入股票代码')
    return
  }
  
  await analyzeStock(stockCode.value.toUpperCase())
  stockCode.value = ''
  selectedStock.value = null
}

async function handleMarketAnalysis(): Promise<void> {
  await analyzeMarket()
}

// ==================== 生命周期 ====================

onMounted(async () => {
  if (!isConnected.value) {
    connect()
  }
  await marketStore.fetchOverview()
})
</script>

<template>
  <div class="dashboard">
    <!-- Hero Section - 顶部搜索区域 -->
    <section class="hero-section">
      <div class="hero-content">
        <!-- 主标题区 -->
        <div class="hero-header">
          <h1 class="hero-title">
            <span class="title-icon">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M12 2L2 7l10 5 10-5-10-5z"/>
                <path d="M2 17l10 5 10-5"/>
                <path d="M2 12l10 5 10-5"/>
              </svg>
            </span>
            智能股票分析助手
          </h1>
          <p class="hero-subtitle">
            基于 AI 的多模态分析引擎，为您提供专业的投资决策支持
          </p>
        </div>
        
        <!-- 搜索框 - ChatGPT 风格 -->
        <div 
          class="search-container"
          :class="{ 'is-focused': isSearchFocused }"
        >
          <div class="search-wrapper">
            <ElIcon class="search-icon" :size="20">
              <Search />
            </ElIcon>
            <input
              v-model="searchQuery"
              type="text"
              class="search-input"
              :placeholder="searchPlaceholder"
              @keyup.enter="handleSearch"
              @focus="isSearchFocused = true"
              @blur="isSearchFocused = false"
            />
            <button 
              class="search-btn"
              :class="{ 'is-loading': isCreating }"
              :disabled="isCreating || !searchQuery.trim()"
              @click="handleSearch"
            >
              <Promotion v-if="!isCreating" />
              <span v-else class="loading-spinner" />
            </button>
          </div>
        </div>
        
        <!-- 快捷操作按钮 -->
        <div class="quick-actions-bar">
          <button 
            class="quick-btn primary"
            :disabled="isCreating"
            @click="handleMarketAnalysis"
          >
            <TrendCharts />
            <span>大盘分析</span>
          </button>
          
          <div class="stock-quick-input">
            <ElAutocomplete
              v-model="stockCode"
              :fetch-suggestions="handleStockSearch"
              placeholder="输入股票代码或名称"
              :trigger-on-focus="false"
              :debounce="300"
              class="stock-autocomplete"
              popper-class="stock-search-popper"
              @select="handleStockSelect"
              @keyup.enter="handleQuickAnalyze"
            >
              <template #default="{ item }">
                <div class="stock-suggestion-item">
                  <span class="stock-name">{{ item.name }}</span>
                  <span class="stock-code">{{ item.ts_code }}</span>
                </div>
              </template>
            </ElAutocomplete>
            <button 
              class="quick-btn success"
              :disabled="isCreating || !stockCode.trim()"
              @click="handleQuickAnalyze"
            >
              <DataAnalysis />
              <span>快速分析</span>
            </button>
          </div>
        </div>
      </div>
      
      <!-- 装饰背景 -->
      <div class="hero-decoration">
        <div class="deco-circle deco-1" />
        <div class="deco-circle deco-2" />
        <div class="deco-grid" />
      </div>
    </section>
    
    <!-- 内容区 -->
    <section class="content-section">
      <div class="content-grid">
        <!-- 左侧主内容 -->
        <div class="main-column">
          <!-- 大盘概览 -->
          <MarketOverviewCard class="dashboard-card animate-scale-in" />
          
          <!-- 活跃任务 -->
          <TaskProgressCard class="dashboard-card animate-scale-in" style="animation-delay: 0.1s" />
          
          <!-- 最近任务 -->
          <RecentTasks class="dashboard-card animate-scale-in" style="animation-delay: 0.2s" />
        </div>
        
        <!-- 右侧边栏 -->
        <div class="side-column">
          <!-- 自选股 -->
          <WatchlistCard class="dashboard-card animate-scale-in" style="animation-delay: 0.15s" />
          
          <!-- 快捷操作 -->
          <QuickActions class="dashboard-card animate-scale-in" style="animation-delay: 0.25s" />
        </div>
      </div>
    </section>
  </div>
</template>

<style scoped lang="scss">
.dashboard {
  min-height: 100vh;
  background: var(--bg-base);
  transition: background-color var(--transition-normal);
}

// ==================== Hero Section ====================

.hero-section {
  position: relative;
  padding: 48px 32px 56px;
  background: var(--gradient-hero);
  overflow: hidden;
}

.hero-content {
  position: relative;
  z-index: 2;
  max-width: 800px;
  margin: 0 auto;
}

.hero-header {
  text-align: center;
  margin-bottom: 40px;
}

.hero-title {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 14px;
  font-size: 34px;
  font-weight: 700;
  color: #fff;
  margin: 0 0 14px 0;
  letter-spacing: -0.5px;
  
  .title-icon {
    width: 42px;
    height: 42px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: rgba(255, 255, 255, 0.15);
    border-radius: 12px;
    
    svg {
      width: 24px;
      height: 24px;
    }
  }
}

.hero-subtitle {
  font-size: 16px;
  color: rgba(255, 255, 255, 0.7);
  margin: 0;
  font-weight: 400;
}

// ==================== 搜索框 (ChatGPT 风格) ====================

.search-container {
  position: relative;
  margin-bottom: 28px;
  
  &::before {
    content: '';
    position: absolute;
    inset: -3px;
    border-radius: 20px;
    background: linear-gradient(135deg, rgba(255,255,255,0.2), rgba(255,255,255,0.05));
    opacity: 0;
    transition: opacity var(--transition-normal);
  }
  
  &.is-focused::before {
    opacity: 1;
  }
  
  &.is-focused {
    .search-wrapper {
      box-shadow: 
        0 0 0 2px rgba(255, 255, 255, 0.2),
        0 8px 32px rgba(0, 0, 0, 0.2);
    }
  }
}

.search-wrapper {
  position: relative;
  display: flex;
  align-items: center;
  background: rgba(255, 255, 255, 0.95);
  border-radius: 16px;
  padding: 6px 8px 6px 20px;
  box-shadow: 0 4px 24px rgba(0, 0, 0, 0.15);
  transition: all var(--transition-normal);
}

.search-icon {
  color: var(--text-tertiary);
  flex-shrink: 0;
}

.search-input {
  flex: 1;
  border: none;
  background: transparent;
  padding: 14px 16px;
  font-size: 16px;
  color: #1a1a2e;  // 输入框背景是白色，文字用深色
  outline: none;
  
  &::placeholder {
    color: #6b7280;  // 固定灰色，确保在白色背景上清晰可见
    opacity: 1;  // 防止浏览器降低透明度
  }
}

.search-btn {
  width: 48px;
  height: 48px;
  border-radius: 12px;
  border: none;
  background: var(--gradient-primary);
  color: #fff;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all var(--transition-fast);
  flex-shrink: 0;
  
  svg {
    width: 20px;
    height: 20px;
  }
  
  &:hover:not(:disabled) {
    transform: scale(1.05);
    box-shadow: 0 4px 16px rgba(59, 130, 246, 0.4);
  }
  
  &:active:not(:disabled) {
    transform: scale(0.98);
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
  
  .loading-spinner {
    width: 20px;
    height: 20px;
    border: 2px solid rgba(255, 255, 255, 0.3);
    border-top-color: #fff;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
  }
}

// ==================== 快捷操作按钮 ====================

.quick-actions-bar {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.quick-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  height: 44px;
  padding: 0 24px;
  border-radius: 12px;
  border: 1px solid rgba(255, 255, 255, 0.2);
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(8px);
  color: #fff;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all var(--transition-fast);
  white-space: nowrap;
  
  svg {
    width: 18px;
    height: 18px;
  }
  
  &:hover:not(:disabled) {
    background: rgba(255, 255, 255, 0.2);
    border-color: rgba(255, 255, 255, 0.3);
    transform: translateY(-2px);
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
  
  &.primary {
    background: rgba(59, 130, 246, 0.3);
    border-color: rgba(59, 130, 246, 0.4);
    
    &:hover:not(:disabled) {
      background: rgba(59, 130, 246, 0.5);
    }
  }
  
  &.success {
    background: rgba(34, 197, 94, 0.3);
    border-color: rgba(34, 197, 94, 0.4);
    
    &:hover:not(:disabled) {
      background: rgba(34, 197, 94, 0.5);
    }
  }
}

.stock-quick-input {
  display: flex;
  gap: 0;
}

.stock-autocomplete {
  width: 180px;
}

// ==================== 装饰背景 ====================

.hero-decoration {
  position: absolute;
  inset: 0;
  pointer-events: none;
  overflow: hidden;
}

.deco-circle {
  position: absolute;
  border-radius: 50%;
  background: radial-gradient(circle, rgba(96, 165, 250, 0.15) 0%, transparent 70%);
  
  &.deco-1 {
    width: 600px;
    height: 600px;
    top: -200px;
    right: -100px;
  }
  
  &.deco-2 {
    width: 400px;
    height: 400px;
    bottom: -150px;
    left: -100px;
    background: radial-gradient(circle, rgba(139, 92, 246, 0.1) 0%, transparent 70%);
  }
}

.deco-grid {
  position: absolute;
  inset: 0;
  background-image: 
    linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px);
  background-size: 60px 60px;
}

// ==================== 内容区 ====================

.content-section {
  padding: 32px;
  margin-top: -24px;
  position: relative;
  z-index: 1;
}

.content-grid {
  display: grid;
  grid-template-columns: 1fr 380px;
  gap: 24px;
  max-width: 1600px;
  margin: 0 auto;
}

.main-column,
.side-column {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.dashboard-card {
  opacity: 0;
  animation: scaleIn 0.4s ease-out forwards;
}

// ==================== 响应式 ====================

@media (max-width: 1280px) {
  .content-grid {
    grid-template-columns: 1fr 320px;
  }
}

@media (max-width: 1024px) {
  .content-grid {
    grid-template-columns: 1fr;
  }
  
  .side-column {
    flex-direction: row;
    flex-wrap: wrap;
    
    > * {
      flex: 1;
      min-width: 300px;
    }
  }
}

@media (max-width: 768px) {
  .hero-section {
    padding: 32px 20px 48px;
  }
  
  .hero-title {
    font-size: 26px;
    
    .title-icon {
      width: 36px;
      height: 36px;
    }
  }
  
  .hero-subtitle {
    font-size: 14px;
  }
  
  .search-wrapper {
    padding: 4px 6px 4px 16px;
  }
  
  .search-input {
    padding: 12px 12px;
    font-size: 15px;
  }
  
  .search-btn {
    width: 42px;
    height: 42px;
  }
  
  .quick-actions-bar {
    flex-direction: column;
    gap: 12px;
    
    .stock-quick-input {
      width: 100%;
      
      .stock-code-input {
        flex: 1;
      }
    }
    
    .quick-btn {
      width: 100%;
      justify-content: center;
    }
  }
  
  .content-section {
    padding: 24px 16px;
  }
  
  .side-column {
    flex-direction: column;
    
    > * {
      min-width: auto;
    }
  }
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@keyframes scaleIn {
  from {
    opacity: 0;
    transform: scale(0.96) translateY(8px);
  }
  to {
    opacity: 1;
    transform: scale(1) translateY(0);
  }
}
</style>

<!-- 全局样式（用于 Element Plus 组件覆盖） -->
<style lang="scss">
// 股票搜索输入框样式覆盖
.stock-autocomplete.el-autocomplete {
  .el-input {
    .el-input__wrapper {
      height: 44px !important;
      padding: 0 20px !important;
      border-radius: 12px 0 0 12px !important;
      border: 1px solid rgba(255, 255, 255, 0.2) !important;
      border-right: none !important;
      background: rgba(255, 255, 255, 0.1) !important;
      backdrop-filter: blur(8px);
      box-shadow: none !important;
      transition: all 0.15s;
      
      &:hover,
      &.is-focus {
        background: rgba(255, 255, 255, 0.15) !important;
        border-color: rgba(255, 255, 255, 0.4) !important;
      }
    }
    
    .el-input__inner {
      height: 100% !important;
      color: #fff !important;
      font-size: 14px !important;
      text-align: center !important;
      -webkit-text-fill-color: #fff !important;
      caret-color: #fff !important;
      
      &::placeholder {
        color: rgba(255, 255, 255, 0.5) !important;
        -webkit-text-fill-color: rgba(255, 255, 255, 0.5) !important;
      }
    }
    
    .el-input__suffix {
      display: none !important;
    }
  }
}

// 快速分析按钮与输入框组合
.stock-quick-input .quick-btn.success {
  border-radius: 0 12px 12px 0 !important;
}

// 下拉弹出层样式
.stock-search-popper {
  &.el-autocomplete__popper {
    border-radius: 12px !important;
    border: 1px solid var(--el-border-color-light) !important;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.12) !important;
    overflow: hidden;
  }
  
  .el-autocomplete-suggestion__wrap {
    max-height: 320px;
    padding: 8px 0;
  }
  
  .el-autocomplete-suggestion li {
    padding: 0;
    
    &:hover,
    &.highlighted {
      background: var(--el-fill-color-light);
    }
  }
  
  .stock-suggestion-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    gap: 16px;
    
    .stock-name {
      font-size: 14px;
      font-weight: 500;
      color: var(--el-text-color-primary);
    }
    
    .stock-code {
      font-size: 13px;
      font-family: 'SF Mono', Consolas, monospace;
      color: var(--el-text-color-secondary);
    }
  }
}
</style>
