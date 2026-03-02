<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useMarketStore } from '@/stores/market'
import { useUserStore } from '@/stores/user'
import { useTask } from '@/hooks'
import { stockApi, subscriptionApi } from '@/api'
import { ElMessage, ElDialog, ElSelect, ElOption, ElTag, ElEmpty } from 'element-plus'
import StockChart from '@/components/charts/StockChart.vue'
import { 
  ArrowLeft, Star, StarFilled, TrendCharts, Bell, Document, 
  Histogram, Clock, ArrowUp, ArrowDown, Refresh
} from '@element-plus/icons-vue'
import type { StockQuote, StrategySubscription, StockDaily } from '@/api/types'

const route = useRoute()
const router = useRouter()
const marketStore = useMarketStore()
const userStore = useUserStore()
const { analyzeStock: doAnalyze, isCreating } = useTask()

const tsCode = computed(() => route.params.code as string)

// ==================== 状态 ====================

const quote = ref<StockQuote | null>(null)
const stockInfo = ref<{
  name?: string
  industry?: string
  ts_code?: string
  area?: string
  market?: string
} | null>(null)
const klineData = ref<StockDaily[]>([])
const loading = ref(true)
const chartLoading = ref(true)

// K线周期
const chartPeriod = ref<'daily' | 'weekly' | 'monthly'>('daily')

// 是否在自选股中
const isInWatchlist = computed(() => (userStore.watchlist || []).includes(tsCode.value))

// 涨跌类型
const priceDirection = computed<'up' | 'down' | 'flat'>(() => {
  if (!quote.value) return 'flat'
  const pctChg = quote.value.pct_chg || 0
  return pctChg > 0 ? 'up' : pctChg < 0 ? 'down' : 'flat'
})

// ==================== 监听订阅相关 ====================

const activeSubscriptions = ref<StrategySubscription[]>([])
const subscribedStrategyIds = ref<Set<string>>(new Set())
const loadingSubscriptions = ref(false)
const subscribeDialogVisible = ref(false)
const selectedStrategyType = ref<string>('')
const addingToSubscription = ref(false)

const isSubscribedToAny = computed(() => subscribedStrategyIds.value.size > 0)

async function loadSubscriptions(): Promise<void> {
  loadingSubscriptions.value = true
  try {
    const subs = await subscriptionApi.getSubscriptions({ is_active: true })
    activeSubscriptions.value = subs
    
    const subscribedTypes = new Set<string>()
    for (const sub of subs) {
      if (sub.watch_list.includes(tsCode.value)) {
        subscribedTypes.add(sub.strategy_type)
      }
    }
    subscribedStrategyIds.value = subscribedTypes
  } catch (error) {
    console.error('加载订阅列表失败:', error)
  } finally {
    loadingSubscriptions.value = false
  }
}

function openSubscribeDialog(): void {
  selectedStrategyType.value = ''
  subscribeDialogVisible.value = true
}

const availableSubscriptions = computed(() => {
  return activeSubscriptions.value.filter(
    sub => !subscribedStrategyIds.value.has(sub.strategy_type)
  )
})

async function addToSubscription(): Promise<void> {
  if (!selectedStrategyType.value) {
    ElMessage.warning('请选择一个策略')
    return
  }
  
  addingToSubscription.value = true
  try {
    const response = await subscriptionApi.addStockToStrategy(
      selectedStrategyType.value,
      tsCode.value
    )
    
    if (response.success) {
      ElMessage.success(response.message)
      subscribedStrategyIds.value.add(selectedStrategyType.value)
      subscribeDialogVisible.value = false
    } else {
      ElMessage.warning(response.message)
    }
  } catch (error) {
    ElMessage.error('添加失败')
    console.error(error)
  } finally {
    addingToSubscription.value = false
  }
}

// ==================== 数据加载 ====================

onMounted(async () => {
  if (!tsCode.value || tsCode.value === 'undefined') {
    ElMessage.error('无效的股票代码')
    loading.value = false
    return
  }
  
  try {
    // 并行加载基本信息和行情
    const [info, quotes] = await Promise.all([
      marketStore.fetchStockBasic(tsCode.value),
      marketStore.fetchQuotes([tsCode.value])
    ])
    
    stockInfo.value = info
    if (quotes.length > 0) {
      quote.value = quotes[0]
    }
    
    // 加载K线数据
    await loadChartData()
    
    // 加载订阅列表
    await loadSubscriptions()
  } finally {
    loading.value = false
  }
})

async function loadChartData() {
  chartLoading.value = true
  try {
    const dailyData = await stockApi.getStockDaily(tsCode.value, { limit: 120 })
    klineData.value = dailyData
  } catch {
    klineData.value = []
  } finally {
    chartLoading.value = false
  }
}

async function refreshData() {
  const quotes = await marketStore.fetchQuotes([tsCode.value])
  if (quotes.length > 0) {
    quote.value = quotes[0]
  }
}

// ==================== 操作 ====================

async function toggleWatchlist(): Promise<void> {
  if (isInWatchlist.value) {
    await userStore.removeFromWatchlist(tsCode.value)
    ElMessage.success('已移出自选')
  } else {
    await userStore.addToWatchlist(tsCode.value)
    ElMessage.success('已加入自选')
  }
}

async function analyzeStock(): Promise<void> {
  const taskId = await doAnalyze(tsCode.value)
  if (taskId) {
    router.push(`/analysis/${taskId}`)
  }
}

// ==================== 格式化 ====================

function formatPrice(val?: number): string {
  if (val === undefined || val === null) return '--'
  return val.toFixed(2)
}

function formatPctChg(val?: number): string {
  if (val === undefined || val === null) return '--'
  const sign = val > 0 ? '+' : ''
  return `${sign}${val.toFixed(2)}%`
}

function formatVol(val?: number): string {
  if (!val) return '--'
  if (val >= 100000000) return (val / 100000000).toFixed(2) + '亿'
  if (val >= 10000) return (val / 10000).toFixed(2) + '万'
  return val.toFixed(0)
}

function formatAmount(val?: number): string {
  if (!val) return '--'
  if (val >= 100000000) return (val / 100000000).toFixed(2) + '亿'
  if (val >= 10000) return (val / 10000).toFixed(2) + '万'
  return val.toFixed(0)
}
</script>

<template>
  <div class="stock-detail-page">
    <!-- 顶部导航 -->
    <header class="top-nav">
      <button class="back-btn" @click="router.back()">
        <ArrowLeft />
        <span>返回</span>
      </button>
      
      <div class="nav-actions">
        <button class="refresh-btn" @click="refreshData" title="刷新数据">
          <Refresh />
        </button>
        
        <button 
          class="watchlist-btn"
          :class="{ 'is-active': isInWatchlist }"
          @click="toggleWatchlist"
        >
          <StarFilled v-if="isInWatchlist" />
          <Star v-else />
          <span>{{ isInWatchlist ? '已关注' : '加自选' }}</span>
        </button>
        
        <button 
          class="analyze-btn"
          :disabled="isCreating"
          @click="analyzeStock"
        >
          <span class="btn-glow" />
          <TrendCharts />
          <span>{{ isCreating ? '分析中...' : 'AI 分析' }}</span>
        </button>
      </div>
    </header>
    
    <!-- 主内容 -->
    <main v-if="!loading" class="main-content">
      <!-- 股票头部信息卡片 -->
      <section class="stock-header-card">
        <div class="header-top">
          <!-- 左侧：股票信息 -->
          <div class="stock-identity">
            <div class="name-row">
              <h1 class="stock-name">{{ stockInfo?.name || '--' }}</h1>
              <span class="stock-code">{{ tsCode }}</span>
            </div>
            <div class="tag-row">
              <span v-if="stockInfo?.industry" class="tag tag-industry">{{ stockInfo.industry }}</span>
              <span v-if="stockInfo?.area" class="tag tag-area">{{ stockInfo.area }}</span>
              
              <!-- 监听状态 -->
              <button 
                v-if="isSubscribedToAny"
                class="subscribe-status subscribed"
                disabled
              >
                <Bell />
                <span>已监听 {{ subscribedStrategyIds.size }} 个策略</span>
              </button>
              <button 
                v-else
                class="subscribe-status"
                :disabled="loadingSubscriptions"
                @click="openSubscribeDialog"
              >
                <Bell />
                <span>加入监听</span>
              </button>
            </div>
          </div>
          
          <!-- 右侧：核心价格 -->
          <div class="price-section" :class="`price-${priceDirection}`">
            <div class="current-price">
              <span class="price-value">{{ formatPrice(quote?.price) }}</span>
              <div class="price-change">
                <span class="change-icon">
                  <ArrowUp v-if="priceDirection === 'up'" />
                  <ArrowDown v-else-if="priceDirection === 'down'" />
                </span>
                <span class="change-value">{{ formatPctChg(quote?.pct_chg) }}</span>
              </div>
            </div>
          </div>
        </div>
        
        <!-- 核心指标 -->
        <div class="metrics-grid">
          <div class="metric-item">
            <span class="metric-label">今开</span>
            <span class="metric-value">{{ formatPrice(quote?.open) }}</span>
          </div>
          <div class="metric-divider" />
          <div class="metric-item">
            <span class="metric-label">最高</span>
            <span class="metric-value price-up">{{ formatPrice(quote?.high) }}</span>
          </div>
          <div class="metric-divider" />
          <div class="metric-item">
            <span class="metric-label">最低</span>
            <span class="metric-value price-down">{{ formatPrice(quote?.low) }}</span>
          </div>
          <div class="metric-divider" />
          <div class="metric-item">
            <span class="metric-label">昨收</span>
            <span class="metric-value">{{ formatPrice(quote?.pre_close) }}</span>
          </div>
          <div class="metric-divider" />
          <div class="metric-item">
            <span class="metric-label">成交量</span>
            <span class="metric-value">{{ formatVol(quote?.vol) }}</span>
          </div>
          <div class="metric-divider" />
          <div class="metric-item">
            <span class="metric-label">成交额</span>
            <span class="metric-value">{{ formatAmount(quote?.amount) }}</span>
          </div>
        </div>
      </section>
      
      <!-- K线图区域 -->
      <section class="chart-section">
        <div class="chart-header">
          <div class="chart-title">
            <Histogram />
            <span>K 线走势</span>
          </div>
          
          <div class="chart-controls">
            <!-- 周期切换 -->
            <div class="period-tabs">
              <button 
                v-for="p in [
                  { value: 'daily', label: '日K' },
                  { value: 'weekly', label: '周K' },
                  { value: 'monthly', label: '月K' },
                ]"
                :key="p.value"
                class="period-tab"
                :class="{ active: chartPeriod === p.value }"
                @click="chartPeriod = p.value as typeof chartPeriod"
              >
                {{ p.label }}
              </button>
            </div>
          </div>
        </div>
        
        <div class="chart-container">
          <!-- 图表加载状态 -->
          <div v-if="chartLoading" class="chart-skeleton">
            <div class="skeleton-bars">
              <div v-for="i in 30" :key="i" class="skeleton-bar" :style="{ height: `${30 + Math.random() * 50}%` }" />
            </div>
          </div>
          
          <!-- K线图 -->
          <StockChart 
            v-else-if="klineData.length > 0" 
            :data="klineData" 
            :ts-code="tsCode" 
          />
          
          <!-- 无数据 -->
          <div v-else class="chart-empty">
            <ElEmpty description="暂无K线数据" :image-size="80" />
          </div>
        </div>
      </section>
      
      <!-- 底部导航磁贴 -->
      <section class="bottom-nav">
        <button 
          class="nav-tile"
          @click="router.push({ name: 'AnalysisList', query: { stock: tsCode } })"
        >
          <div class="tile-icon history">
            <Clock />
          </div>
          <div class="tile-content">
            <span class="tile-title">历史分析</span>
            <span class="tile-desc">查看 AI 分析记录</span>
          </div>
        </button>
        
        <button class="nav-tile">
          <div class="tile-icon news">
            <Document />
          </div>
          <div class="tile-content">
            <span class="tile-title">相关新闻</span>
            <span class="tile-desc">最新资讯动态</span>
          </div>
        </button>
        
        <button class="nav-tile">
          <div class="tile-icon finance">
            <Histogram />
          </div>
          <div class="tile-content">
            <span class="tile-title">财务数据</span>
            <span class="tile-desc">财报与指标</span>
          </div>
        </button>
      </section>
    </main>
    
    <!-- 加载骨架 -->
    <div v-else class="loading-skeleton">
      <div class="skeleton-header">
        <div class="skeleton-line w-40 h-8" />
        <div class="skeleton-line w-24 h-4 mt-2" />
        <div class="skeleton-line w-64 h-12 mt-4" />
      </div>
      <div class="skeleton-chart">
        <div class="skeleton-line w-full h-96" />
      </div>
    </div>
    
    <!-- 加入监听策略弹窗 -->
    <ElDialog
      v-model="subscribeDialogVisible"
      title="加入监听策略"
      width="480px"
      :close-on-click-modal="false"
      class="subscribe-dialog"
    >
      <div class="dialog-body">
        <p class="dialog-hint">
          将 <strong>{{ stockInfo?.name || tsCode }}</strong> 添加到以下策略的监听列表：
        </p>
        
        <ElSelect
          v-model="selectedStrategyType"
          placeholder="选择策略"
          class="strategy-select"
          size="large"
        >
          <ElOption
            v-for="sub in availableSubscriptions"
            :key="sub.strategy_type"
            :label="sub.strategy_name"
            :value="sub.strategy_type"
          >
            <div class="strategy-option">
              <span class="strategy-name">{{ sub.strategy_name }}</span>
              <ElTag size="small" type="info">{{ sub.strategy_type }}</ElTag>
            </div>
          </ElOption>
        </ElSelect>
        
        <ElEmpty 
          v-if="availableSubscriptions.length === 0" 
          description="暂无可用策略"
          :image-size="60"
        />
        
        <div v-if="subscribedStrategyIds.size > 0" class="subscribed-list">
          <p>该股票已在以下策略中监听：</p>
          <div class="tag-list">
            <ElTag 
              v-for="sub in activeSubscriptions.filter(s => subscribedStrategyIds.has(s.strategy_type))"
              :key="sub.strategy_type"
              size="small"
              type="success"
            >
              {{ sub.strategy_name }}
            </ElTag>
          </div>
        </div>
      </div>
      
      <template #footer>
        <div class="dialog-actions">
          <button class="btn-cancel" @click="subscribeDialogVisible = false">取消</button>
          <button 
            class="btn-confirm"
            :disabled="!selectedStrategyType || availableSubscriptions.length === 0 || addingToSubscription"
            @click="addToSubscription"
          >
            {{ addingToSubscription ? '添加中...' : '确认添加' }}
          </button>
        </div>
      </template>
    </ElDialog>
  </div>
</template>

<style lang="scss" scoped>
// ==================== 专业金融配色 ====================

$color-up: #f23645;
$color-down: #089981;
$color-flat: #64748b;
$color-primary: #3b82f6;
$color-ai: linear-gradient(135deg, #8b5cf6, #6366f1);

// ==================== 页面布局 ====================

.stock-detail-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 20px 24px;
}

// ==================== 顶部导航 ====================

.top-nav {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.back-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 10px 16px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-light);
  border-radius: 10px;
  color: var(--text-secondary);
  font-size: 14px;
  cursor: pointer;
  transition: all 0.2s;
  
  svg { width: 16px; height: 16px; }
  
  &:hover {
    background: var(--bg-tertiary);
    color: var(--text-primary);
  }
}

.nav-actions {
  display: flex;
  align-items: center;
  gap: 10px;
}

.refresh-btn {
  width: 40px;
  height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-secondary);
  border: 1px solid var(--border-light);
  border-radius: 10px;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.2s;
  
  svg { width: 18px; height: 18px; }
  
  &:hover {
    color: $color-primary;
    border-color: $color-primary;
  }
}

.watchlist-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 10px 18px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-light);
  border-radius: 10px;
  color: var(--text-secondary);
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  
  svg { width: 16px; height: 16px; }
  
  &:hover {
    background: rgba(#f59e0b, 0.1);
    border-color: #f59e0b;
    color: #f59e0b;
  }
  
  &.is-active {
    background: rgba(#f59e0b, 0.1);
    border-color: #f59e0b;
    color: #f59e0b;
    
    svg { color: #f59e0b; }
  }
}

.analyze-btn {
  position: relative;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 10px 22px;
  background: $color-ai;
  border: none;
  border-radius: 10px;
  color: #fff;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  overflow: hidden;
  transition: all 0.2s;
  
  svg { width: 18px; height: 18px; }
  
  .btn-glow {
    position: absolute;
    inset: 0;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
    animation: glowSweep 2s ease-in-out infinite;
  }
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(#8b5cf6, 0.4);
  }
  
  &:disabled {
    opacity: 0.7;
    cursor: not-allowed;
    transform: none;
  }
}

@keyframes glowSweep {
  0% { transform: translateX(-100%); }
  50%, 100% { transform: translateX(100%); }
}

// ==================== 股票头部卡片 ====================

.stock-header-card {
  background: var(--bg-card);
  border-radius: 16px;
  border: 1px solid var(--border-light);
  padding: 24px 28px;
  margin-bottom: 20px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
}

.header-top {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 24px;
  gap: 24px;
}

.stock-identity {
  flex: 1;
}

.name-row {
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 12px;
}

.stock-name {
  font-size: 28px;
  font-weight: 700;
  color: var(--text-primary);
  margin: 0;
  letter-spacing: -0.5px;
}

.stock-code {
  font-size: 14px;
  font-family: var(--font-mono);
  color: var(--text-tertiary);
  background: var(--bg-tertiary);
  padding: 4px 10px;
  border-radius: 6px;
}

.tag-row {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.tag {
  font-size: 12px;
  padding: 4px 12px;
  border-radius: 6px;
  
  &.tag-industry {
    background: rgba($color-primary, 0.1);
    color: $color-primary;
  }
  
  &.tag-area {
    background: var(--bg-tertiary);
    color: var(--text-secondary);
  }
}

.subscribe-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 14px;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  background: rgba($color-primary, 0.1);
  border: 1px solid rgba($color-primary, 0.2);
  color: $color-primary;
  
  svg { width: 14px; height: 14px; }
  
  &:hover:not(:disabled) {
    background: rgba($color-primary, 0.15);
  }
  
  &.subscribed {
    background: rgba($color-down, 0.1);
    border-color: rgba($color-down, 0.2);
    color: $color-down;
    cursor: default;
  }
}

// 价格区域
.price-section {
  text-align: right;
  
  &.price-up {
    .price-value, .change-value, .change-icon { color: $color-up; }
  }
  &.price-down {
    .price-value, .change-value, .change-icon { color: $color-down; }
  }
  &.price-flat {
    .price-value, .change-value { color: $color-flat; }
  }
}

.current-price {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
}

.price-value {
  font-size: 42px;
  font-weight: 800;
  font-family: 'DIN Alternate', 'Roboto Mono', var(--font-mono);
  letter-spacing: -1px;
  line-height: 1;
}

.price-change {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 18px;
  font-weight: 600;
}

.change-icon {
  svg { width: 16px; height: 16px; }
}

// 指标网格
.metrics-grid {
  display: flex;
  align-items: center;
  gap: 0;
  padding: 16px 0 0;
  border-top: 1px solid var(--border-light);
}

.metric-item {
  flex: 1;
  text-align: center;
  
  .metric-label {
    display: block;
    font-size: 12px;
    color: var(--text-tertiary);
    margin-bottom: 6px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  
  .metric-value {
    font-size: 16px;
    font-weight: 600;
    font-family: var(--font-mono);
    color: var(--text-primary);
    
    &.price-up { color: $color-up; }
    &.price-down { color: $color-down; }
  }
}

.metric-divider {
  width: 1px;
  height: 36px;
  background: var(--border-light);
}

// ==================== K线图区域 ====================

.chart-section {
  background: var(--bg-card);
  border-radius: 16px;
  border: 1px solid var(--border-light);
  overflow: hidden;
  margin-bottom: 20px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
}

.chart-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 24px;
  border-bottom: 1px solid var(--border-light);
  background: var(--bg-secondary);
}

.chart-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 16px;
  font-weight: 600;
  color: var(--text-primary);
  
  svg { width: 18px; height: 18px; color: var(--text-tertiary); }
}

.chart-controls {
  display: flex;
  align-items: center;
  gap: 12px;
}

.period-tabs {
  display: flex;
  background: var(--bg-card);
  border-radius: 8px;
  padding: 4px;
  border: 1px solid var(--border-light);
}

.period-tab {
  padding: 6px 16px;
  border: none;
  background: transparent;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 500;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    color: var(--text-primary);
  }
  
  &.active {
    background: $color-primary;
    color: #fff;
  }
}

.chart-container {
  height: 420px;
  padding: 20px;
}

.chart-skeleton {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: flex-end;
  gap: 4px;
  padding: 20px;
}

.skeleton-bars {
  display: flex;
  align-items: flex-end;
  gap: 4px;
  width: 100%;
  height: 100%;
}

.skeleton-bar {
  flex: 1;
  background: linear-gradient(180deg, var(--bg-tertiary) 0%, var(--bg-secondary) 100%);
  border-radius: 2px;
  animation: skeletonPulse 1.5s ease-in-out infinite;
}

@keyframes skeletonPulse {
  0%, 100% { opacity: 0.6; }
  50% { opacity: 1; }
}

.chart-empty {
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
}

// ==================== 底部导航磁贴 ====================

.bottom-nav {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

.nav-tile {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 20px 24px;
  background: var(--bg-card);
  border: 1px solid var(--border-light);
  border-radius: 14px;
  cursor: pointer;
  transition: all 0.2s;
  text-align: left;
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(0, 0, 0, 0.08);
    border-color: var(--border-color);
  }
}

.tile-icon {
  width: 48px;
  height: 48px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  
  svg { width: 24px; height: 24px; }
  
  &.history {
    background: linear-gradient(135deg, rgba(#8b5cf6, 0.15), rgba(#6366f1, 0.1));
    color: #8b5cf6;
  }
  
  &.news {
    background: linear-gradient(135deg, rgba(#f59e0b, 0.15), rgba(#f97316, 0.1));
    color: #f59e0b;
  }
  
  &.finance {
    background: linear-gradient(135deg, rgba($color-down, 0.15), rgba(#10b981, 0.1));
    color: $color-down;
  }
}

.tile-content {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.tile-title {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
}

.tile-desc {
  font-size: 12px;
  color: var(--text-tertiary);
}

// ==================== 加载骨架 ====================

.loading-skeleton {
  padding: 40px;
}

.skeleton-header {
  margin-bottom: 24px;
}

.skeleton-line {
  background: linear-gradient(90deg, var(--bg-tertiary) 25%, var(--bg-secondary) 50%, var(--bg-tertiary) 75%);
  background-size: 200% 100%;
  animation: shimmer 1.5s infinite;
  border-radius: 8px;
  
  &.w-24 { width: 96px; }
  &.w-40 { width: 160px; }
  &.w-64 { width: 256px; }
  &.w-full { width: 100%; }
  &.h-4 { height: 16px; }
  &.h-8 { height: 32px; }
  &.h-12 { height: 48px; }
  &.h-96 { height: 384px; }
  &.mt-2 { margin-top: 8px; }
  &.mt-4 { margin-top: 16px; }
}

@keyframes shimmer {
  from { background-position: -200% 0; }
  to { background-position: 200% 0; }
}

// ==================== 弹窗样式 ====================

:deep(.subscribe-dialog) {
  .el-dialog {
    border-radius: 16px;
  }
  
  .el-dialog__header {
    padding: 20px 24px;
    border-bottom: 1px solid var(--border-light);
  }
  
  .el-dialog__body {
    padding: 24px;
  }
  
  .el-dialog__footer {
    padding: 16px 24px 20px;
    border-top: 1px solid var(--border-light);
  }
}

.dialog-body {
  .dialog-hint {
    font-size: 14px;
    color: var(--text-secondary);
    margin-bottom: 16px;
    
    strong { color: var(--text-primary); }
  }
}

.strategy-select {
  width: 100%;
  margin-bottom: 16px;
}

.strategy-option {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.strategy-name {
  font-weight: 500;
}

.subscribed-list {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid var(--border-light);
  
  p {
    font-size: 13px;
    color: var(--text-tertiary);
    margin-bottom: 8px;
  }
}

.tag-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.dialog-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}

.btn-cancel {
  padding: 10px 20px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 10px;
  font-size: 14px;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-tertiary);
    color: var(--text-primary);
  }
}

.btn-confirm {
  padding: 10px 24px;
  background: $color-primary;
  border: none;
  border-radius: 10px;
  font-size: 14px;
  font-weight: 500;
  color: #fff;
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover:not(:disabled) {
    background: #2563eb;
  }
  
  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
}

// ==================== 响应式 ====================

@media (max-width: 900px) {
  .header-top {
    flex-direction: column;
    gap: 20px;
  }
  
  .price-section {
    text-align: left;
  }
  
  .current-price {
    align-items: flex-start;
  }
  
  .metrics-grid {
    flex-wrap: wrap;
    gap: 16px;
  }
  
  .metric-divider {
    display: none;
  }
  
  .metric-item {
    flex: 0 0 calc(33.33% - 12px);
    text-align: left;
  }
  
  .bottom-nav {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 640px) {
  .stock-detail-page {
    padding: 16px;
  }
  
  .top-nav {
    flex-direction: column;
    align-items: stretch;
    gap: 12px;
  }
  
  .nav-actions {
    justify-content: flex-end;
  }
  
  .price-value {
    font-size: 36px;
  }
  
  .chart-container {
    height: 320px;
  }
}
</style>
