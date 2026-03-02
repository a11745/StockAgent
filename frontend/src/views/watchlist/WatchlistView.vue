<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { useMarketStore } from '@/stores/market'
import { useTask } from '@/hooks'
import { stockApi } from '@/api'
import { ElMessage, ElDialog, ElAutocomplete, ElDropdown, ElDropdownMenu, ElDropdownItem } from 'element-plus'
import { Plus, Delete, Search, ArrowUp, ArrowDown, TrendCharts, View, Sort, Refresh, Star, Loading } from '@element-plus/icons-vue'
import type { StockQuote, StockBasic } from '@/api/types'

const router = useRouter()
const userStore = useUserStore()
const marketStore = useMarketStore()
const { analyzeStock: doAnalyze, isCreating } = useTask()

// ==================== 状态 ====================

// 自选股行情
const quotes = ref<Map<string, StockQuote>>(new Map())
const loading = ref(true)

// 搜索和排序
const filterKeyword = ref('')
const sortBy = ref<'default' | 'pct_chg_asc' | 'pct_chg_desc' | 'vol_desc'>('default')

// 悬停行
const hoveredRow = ref<string | null>(null)

// 价格闪烁动画
const flashingPrices = ref<Set<string>>(new Set())

// 自选股列表
const watchlist = computed(() => userStore.watchlist)

// 筛选和排序后的列表
const filteredWatchlist = computed(() => {
  let list = [...watchlist.value]
  
  // 关键词过滤
  if (filterKeyword.value.trim()) {
    const keyword = filterKeyword.value.toLowerCase()
    list = list.filter(code => {
      const quote = quotes.value.get(code)
      return code.toLowerCase().includes(keyword) || 
             (quote?.name || '').toLowerCase().includes(keyword)
    })
  }
  
  // 排序
  if (sortBy.value !== 'default') {
    list.sort((a, b) => {
      const quoteA = quotes.value.get(a)
      const quoteB = quotes.value.get(b)
      
      if (sortBy.value === 'pct_chg_desc') {
        return (quoteB?.pct_chg || 0) - (quoteA?.pct_chg || 0)
      } else if (sortBy.value === 'pct_chg_asc') {
        return (quoteA?.pct_chg || 0) - (quoteB?.pct_chg || 0)
      } else if (sortBy.value === 'vol_desc') {
        return (quoteB?.vol || 0) - (quoteA?.vol || 0)
      }
      return 0
    })
  }
  
  return list
})

// 添加股票弹窗
const addDialogVisible = ref(false)
const searchKeyword = ref('')
const selectedStock = ref<StockBasic | null>(null)
const isAdding = ref(false)

// ==================== 生命周期 ====================

onMounted(async () => {
  await loadQuotes()
})

async function loadQuotes() {
  if (watchlist.value.length > 0) {
    loading.value = true
    try {
      const quoteList = await marketStore.fetchQuotes(watchlist.value)
      quoteList.forEach((q) => quotes.value.set(q.ts_code, q))
    } finally {
      loading.value = false
    }
  } else {
    loading.value = false
  }
}

// 刷新行情
async function refreshQuotes() {
  if (watchlist.value.length === 0) return
  
  const oldQuotes = new Map(quotes.value)
  const quoteList = await marketStore.fetchQuotes(watchlist.value)
  
  quoteList.forEach((q) => {
    const oldPrice = oldQuotes.get(q.ts_code)?.price
    quotes.value.set(q.ts_code, q)
    
    // 价格变化时触发闪烁
    if (oldPrice && oldPrice !== q.price) {
      triggerPriceFlash(q.ts_code)
    }
  })
}

// 价格闪烁动画
function triggerPriceFlash(tsCode: string) {
  flashingPrices.value.add(tsCode)
  setTimeout(() => {
    flashingPrices.value.delete(tsCode)
  }, 600)
}

// ==================== 行情相关 ====================

function getQuote(tsCode: string): StockQuote | undefined {
  return quotes.value.get(tsCode)
}

function getPriceClass(pctChg?: number): 'up' | 'down' | 'flat' {
  if (!pctChg) return 'flat'
  return pctChg > 0 ? 'up' : pctChg < 0 ? 'down' : 'flat'
}

function formatPctChg(pctChg?: number): string {
  if (pctChg === undefined || pctChg === null) return '-'
  const sign = pctChg > 0 ? '+' : ''
  return `${sign}${pctChg.toFixed(2)}%`
}

function formatVol(vol?: number): string {
  if (!vol) return '-'
  if (vol >= 100000000) return (vol / 100000000).toFixed(2) + '亿'
  if (vol >= 10000) return (vol / 10000).toFixed(2) + '万'
  return vol.toFixed(0)
}

function formatAmount(amount?: number): string {
  if (!amount) return '-'
  if (amount >= 100000000) return (amount / 100000000).toFixed(2) + '亿'
  if (amount >= 10000) return (amount / 10000).toFixed(2) + '万'
  return amount.toFixed(0)
}

// ==================== 操作 ====================

async function removeFromWatchlist(e: Event, tsCode: string) {
  e.stopPropagation()
  await userStore.removeFromWatchlist(tsCode)
  quotes.value.delete(tsCode)
  ElMessage.success('已移除')
}

async function analyzeStock(e: Event, tsCode: string) {
  e.stopPropagation()
  const taskId = await doAnalyze(tsCode)
  if (taskId) {
    router.push(`/analysis/${taskId}`)
  }
}

function viewDetail(e: Event, tsCode: string) {
  e.stopPropagation()
  router.push(`/stock/${tsCode}`)
}

// ==================== 添加股票弹窗 ====================

function openAddDialog() {
  addDialogVisible.value = true
  searchKeyword.value = ''
  selectedStock.value = null
}

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

function handleStockSelect(stock: Record<string, any>): void {
  selectedStock.value = stock as StockBasic
  searchKeyword.value = stock.ts_code || ''
}

async function handleAddStock(): Promise<void> {
  if (!selectedStock.value) {
    ElMessage.warning('请先选择一只股票')
    return
  }
  
  const tsCode = selectedStock.value.ts_code
  
  if (watchlist.value.includes(tsCode)) {
    ElMessage.warning('该股票已在自选股中')
    return
  }
  
  isAdding.value = true
  try {
    const success = await userStore.addToWatchlist(tsCode)
    if (success) {
      ElMessage.success(`已添加 ${selectedStock.value.name}`)
      addDialogVisible.value = false
      
      // 刷新行情
      const quoteList = await marketStore.fetchQuotes([tsCode])
      quoteList.forEach((q) => quotes.value.set(q.ts_code, q))
    } else {
      ElMessage.error('添加失败')
    }
  } finally {
    isAdding.value = false
    searchKeyword.value = ''
    selectedStock.value = null
  }
}

// 排序选项
const sortOptions = [
  { label: '默认排序', value: 'default' },
  { label: '涨幅从高到低', value: 'pct_chg_desc' },
  { label: '涨幅从低到高', value: 'pct_chg_asc' },
  { label: '成交量从高到低', value: 'vol_desc' },
]

function getSortLabel(): string {
  return sortOptions.find(o => o.value === sortBy.value)?.label || '排序'
}
</script>

<template>
  <div class="watchlist-page">
    <!-- 页面标题 -->
    <header class="page-header">
      <div class="header-left">
        <div class="title-section">
          <ElIcon class="title-icon"><Star /></ElIcon>
          <h1>自选股</h1>
        </div>
        <span class="stock-count">{{ filteredWatchlist.length }} 只股票</span>
      </div>
    </header>
    
    <!-- 工具栏 -->
    <div class="toolbar">
      <div class="toolbar-left">
        <!-- 搜索框 -->
        <div class="search-box">
          <ElIcon class="search-icon"><Search /></ElIcon>
          <input 
            v-model="filterKeyword"
            type="text" 
            placeholder="搜索股票代码或名称..."
            class="search-input"
          />
        </div>
        
        <!-- 排序 -->
        <ElDropdown trigger="click" @command="(cmd: string) => sortBy = cmd as typeof sortBy">
          <button class="sort-btn">
            <ElIcon><Sort /></ElIcon>
            <span>{{ getSortLabel() }}</span>
          </button>
          <template #dropdown>
            <ElDropdownMenu>
              <ElDropdownItem 
                v-for="opt in sortOptions" 
                :key="opt.value" 
                :command="opt.value"
                :class="{ active: sortBy === opt.value }"
              >
                {{ opt.label }}
              </ElDropdownItem>
            </ElDropdownMenu>
          </template>
        </ElDropdown>
      </div>
      
      <div class="toolbar-right">
        <!-- 刷新 -->
        <button class="icon-btn" @click="refreshQuotes" title="刷新行情">
          <ElIcon><Refresh /></ElIcon>
        </button>
        
        <!-- 添加股票 -->
        <button class="add-btn" @click="openAddDialog">
          <ElIcon><Plus /></ElIcon>
          <span>添加股票</span>
        </button>
      </div>
    </div>
    
    <!-- 主体内容 -->
    <div class="content-area">
      <!-- 加载骨架屏 -->
      <div v-if="loading" class="skeleton-list">
        <div v-for="i in 5" :key="i" class="skeleton-row">
          <div class="skeleton-cell name">
            <div class="skeleton-line w-20" />
            <div class="skeleton-line w-16 mt-1" />
          </div>
          <div class="skeleton-cell price">
            <div class="skeleton-line w-16" />
          </div>
          <div class="skeleton-cell change">
            <div class="skeleton-line w-14" />
          </div>
          <div class="skeleton-cell vol">
            <div class="skeleton-line w-20" />
          </div>
        </div>
      </div>
      
      <!-- 股票列表 -->
      <div v-else-if="filteredWatchlist.length > 0" class="stock-list">
        <!-- 表头 -->
        <div class="list-header">
          <div class="col col-name">股票</div>
          <div class="col col-price">最新价</div>
          <div class="col col-change">涨跌幅</div>
          <div class="col col-vol">成交量</div>
          <div class="col col-amount">成交额</div>
          <div class="col col-actions">操作</div>
        </div>
        
        <!-- 数据行 -->
        <div 
          v-for="(tsCode, index) in filteredWatchlist"
          :key="tsCode"
          class="list-row"
          :class="{ 'is-hovered': hoveredRow === tsCode }"
          :style="{ animationDelay: `${index * 0.03}s` }"
          @mouseenter="hoveredRow = tsCode"
          @mouseleave="hoveredRow = null"
          @click="router.push(`/stock/${tsCode}`)"
        >
          <!-- 股票名称 -->
          <div class="col col-name">
            <div class="stock-info">
              <span class="stock-name">{{ getQuote(tsCode)?.name || '-' }}</span>
              <span class="stock-code">{{ tsCode }}</span>
            </div>
          </div>
          
          <!-- 最新价 -->
          <div class="col col-price">
            <span 
              class="price-value"
              :class="[
                `price-${getPriceClass(getQuote(tsCode)?.pct_chg)}`,
                { 'is-flashing': flashingPrices.has(tsCode) }
              ]"
            >
              {{ getQuote(tsCode)?.price?.toFixed(2) || '-' }}
            </span>
          </div>
          
          <!-- 涨跌幅 -->
          <div class="col col-change">
            <div 
              class="change-badge"
              :class="`change-${getPriceClass(getQuote(tsCode)?.pct_chg)}`"
            >
              <ElIcon v-if="(getQuote(tsCode)?.pct_chg || 0) > 0" class="change-icon"><ArrowUp /></ElIcon>
              <ElIcon v-else-if="(getQuote(tsCode)?.pct_chg || 0) < 0" class="change-icon"><ArrowDown /></ElIcon>
              <span>{{ formatPctChg(getQuote(tsCode)?.pct_chg) }}</span>
            </div>
          </div>
          
          <!-- 成交量 -->
          <div class="col col-vol">
            <span class="vol-value">{{ formatVol(getQuote(tsCode)?.vol) }}</span>
          </div>
          
          <!-- 成交额 -->
          <div class="col col-amount">
            <span class="amount-value">{{ formatAmount(getQuote(tsCode)?.amount) }}</span>
          </div>
          
          <!-- 操作按钮 -->
          <div class="col col-actions">
            <div class="action-buttons" :class="{ 'is-visible': hoveredRow === tsCode }">
              <button 
                class="action-btn analyze-btn"
                :disabled="isCreating"
                @click="analyzeStock($event, tsCode)"
                title="AI 分析"
              >
                <span class="ai-pulse" />
                <TrendCharts />
              </button>
              <button 
                class="action-btn view-btn"
                @click="viewDetail($event, tsCode)"
                title="查看详情"
              >
                <View />
              </button>
              <button 
                class="action-btn delete-btn"
                @click="removeFromWatchlist($event, tsCode)"
                title="移除"
              >
                <Delete />
              </button>
            </div>
          </div>
        </div>
      </div>
      
      <!-- 空状态 -->
      <div v-else class="empty-state">
        <div class="empty-icon">
          <Star />
        </div>
        <h3>暂无自选股</h3>
        <p>添加您关注的股票，随时跟踪行情变化</p>
        <button class="empty-add-btn" @click="openAddDialog">
          <Plus />
          添加第一只股票
        </button>
      </div>
    </div>
    
    <!-- 添加股票弹窗 -->
    <ElDialog
      v-model="addDialogVisible"
      title="添加自选股"
      width="480px"
      :close-on-click-modal="false"
      class="add-dialog"
    >
      <div class="dialog-body">
        <div class="search-section">
          <label>搜索股票</label>
          <ElAutocomplete
            v-model="searchKeyword"
            :fetch-suggestions="handleStockSearch"
            placeholder="输入股票代码或名称"
            clearable
            class="dialog-search"
            value-key="ts_code"
            :debounce="300"
            @select="handleStockSelect"
          >
            <template #prefix>
              <ElIcon><Search /></ElIcon>
            </template>
            <template #default="{ item }">
              <div class="suggestion-item">
                <div class="suggestion-main">
                  <span class="suggestion-name">{{ item.name }}</span>
                  <span class="suggestion-industry" v-if="item.industry">{{ item.industry }}</span>
                </div>
                <span class="suggestion-code">{{ item.ts_code }}</span>
              </div>
            </template>
          </ElAutocomplete>
        </div>
        
        <!-- 选中预览 -->
        <div v-if="selectedStock" class="selected-preview">
          <div class="preview-header">
            <span class="preview-name">{{ selectedStock.name }}</span>
            <span class="preview-code">{{ selectedStock.ts_code }}</span>
          </div>
          <div class="preview-meta">
            <span v-if="selectedStock.industry" class="meta-tag">{{ selectedStock.industry }}</span>
            <span v-if="selectedStock.area" class="meta-tag">{{ selectedStock.area }}</span>
          </div>
        </div>
      </div>
      
      <template #footer>
        <div class="dialog-actions">
          <button class="btn-cancel" @click="addDialogVisible = false">取消</button>
          <button 
            class="btn-confirm"
            :disabled="!selectedStock || isAdding"
            @click="handleAddStock"
          >
            <Loading v-if="isAdding" class="spinning" />
            <span>{{ isAdding ? '添加中...' : '确认添加' }}</span>
          </button>
        </div>
      </template>
    </ElDialog>
  </div>
</template>

<style lang="scss" scoped>
// ==================== 变量 ====================

$color-up: #ef4444;
$color-down: #22c55e;
$color-flat: #64748b;
$color-primary: #3b82f6;

// ==================== 页面布局 ====================

.watchlist-page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 24px 32px;
}

// ==================== 页面标题 ====================

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.header-left {
  display: flex;
  align-items: baseline;
  gap: 16px;
}

.title-section {
  display: flex;
  align-items: center;
  gap: 10px;
  
  .title-icon {
    font-size: 24px;
    color: var(--warning);
  }
  
  h1 {
    font-size: 26px;
    font-weight: 700;
    color: var(--text-primary);
    margin: 0;
    letter-spacing: -0.5px;
  }
}

.stock-count {
  font-size: 14px;
  color: var(--text-tertiary);
  background: var(--bg-tertiary);
  padding: 4px 12px;
  border-radius: 12px;
}

// ==================== 工具栏 ====================

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  padding: 16px 20px;
  background: var(--bg-card);
  border-radius: 14px;
  border: 1px solid var(--border-light);
  margin-bottom: 16px;
}

.toolbar-left {
  display: flex;
  align-items: center;
  gap: 12px;
}

.search-box {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 16px;
  background: var(--bg-secondary);
  border-radius: 10px;
  border: 1px solid transparent;
  transition: all 0.2s;
  min-width: 280px;
  
  &:focus-within {
    border-color: $color-primary;
    background: var(--bg-card);
    box-shadow: 0 0 0 3px rgba($color-primary, 0.1);
  }
  
  .search-icon {
    color: var(--text-tertiary);
    font-size: 16px;
  }
  
  .search-input {
    flex: 1;
    border: none;
    background: transparent;
    outline: none;
    font-size: 14px;
    color: var(--text-primary);
    
    &::placeholder {
      color: var(--text-tertiary);
    }
  }
}

.sort-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 10px 16px;
  background: var(--bg-secondary);
  border: 1px solid transparent;
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

.toolbar-right {
  display: flex;
  align-items: center;
  gap: 10px;
}

.icon-btn {
  width: 40px;
  height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-secondary);
  border: none;
  border-radius: 10px;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-tertiary);
    color: $color-primary;
  }
}

.add-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 20px;
  background: var(--primary-500);
  border: none;
  border-radius: 10px;
  color: var(--text-inverse);
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--primary-600);
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
  }
}

// ==================== 骨架屏 ====================

.skeleton-list {
  background: var(--bg-card);
  border-radius: 14px;
  border: 1px solid var(--border-light);
  overflow: hidden;
}

.skeleton-row {
  display: flex;
  align-items: center;
  padding: 20px 24px;
  border-bottom: 1px solid var(--border-light);
  
  &:last-child {
    border-bottom: none;
  }
}

.skeleton-cell {
  &.name { flex: 2; }
  &.price { flex: 1; }
  &.change { flex: 1; }
  &.vol { flex: 1; }
}

.skeleton-line {
  height: 14px;
  background: linear-gradient(90deg, var(--bg-tertiary) 25%, var(--bg-secondary) 50%, var(--bg-tertiary) 75%);
  background-size: 200% 100%;
  animation: shimmer 1.5s infinite;
  border-radius: 4px;
  
  &.w-14 { width: 56px; }
  &.w-16 { width: 64px; }
  &.w-20 { width: 80px; }
  &.mt-1 { margin-top: 6px; }
}

@keyframes shimmer {
  from { background-position: -200% 0; }
  to { background-position: 200% 0; }
}

// ==================== 股票列表 ====================

.stock-list {
  background: var(--bg-card);
  border-radius: 14px;
  border: 1px solid var(--border-light);
  overflow: hidden;
}

.list-header {
  display: flex;
  align-items: center;
  padding: 14px 24px;
  background: var(--bg-secondary);
  border-bottom: 1px solid var(--border-light);
  
  .col {
    font-size: 12px;
    font-weight: 600;
    color: var(--text-tertiary);
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
}

.list-row {
  display: flex;
  align-items: center;
  padding: 16px 24px;
  border-bottom: 1px solid var(--border-light);
  cursor: pointer;
  transition: all 0.15s;
  animation: fadeSlideIn 0.3s ease-out both;
  
  &:last-child {
    border-bottom: none;
  }
  
  &:hover,
  &.is-hovered {
    background: linear-gradient(90deg, rgba($color-primary, 0.03), transparent);
  }
}

@keyframes fadeSlideIn {
  from {
    opacity: 0;
    transform: translateY(8px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.col {
  &.col-name { flex: 2; min-width: 180px; }
  &.col-price { flex: 1; min-width: 100px; text-align: right; }
  &.col-change { flex: 1; min-width: 120px; display: flex; justify-content: center; }
  &.col-vol { flex: 1; min-width: 100px; text-align: right; }
  &.col-amount { flex: 1; min-width: 100px; text-align: right; }
  &.col-actions { width: 120px; display: flex; justify-content: flex-end; }
}

// 股票名称
.stock-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.stock-name {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
}

.stock-code {
  font-size: 12px;
  font-family: var(--font-mono);
  color: var(--text-tertiary);
  background: var(--bg-tertiary);
  padding: 2px 8px;
  border-radius: 4px;
  display: inline-block;
  width: fit-content;
}

// 最新价
.price-value {
  font-size: 16px;
  font-weight: 700;
  font-family: var(--font-mono);
  transition: all 0.15s;
  padding: 4px 8px;
  border-radius: 6px;
  
  &.price-up { color: $color-up; }
  &.price-down { color: $color-down; }
  &.price-flat { color: $color-flat; }
  
  &.is-flashing {
    animation: priceFlash 0.6s ease-out;
  }
}

@keyframes priceFlash {
  0% { background: transparent; }
  30% { background: rgba($color-up, 0.15); }
  100% { background: transparent; }
}

// 涨跌幅
.change-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 6px 12px;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 600;
  font-family: var(--font-mono);
  
  .change-icon {
    font-size: 12px;
  }
  
  &.change-up {
    background: rgba($color-up, 0.1);
    color: $color-up;
  }
  
  &.change-down {
    background: rgba($color-down, 0.1);
    color: $color-down;
  }
  
  &.change-flat {
    background: var(--bg-tertiary);
    color: $color-flat;
  }
}

// 成交量/额
.vol-value,
.amount-value {
  font-size: 14px;
  font-family: var(--font-mono);
  color: var(--text-secondary);
}

// ==================== 操作按钮 ====================

.action-buttons {
  display: flex;
  gap: 6px;
  opacity: 0;
  transform: translateX(8px);
  transition: all 0.2s;
  
  &.is-visible {
    opacity: 1;
    transform: translateX(0);
  }
}

.action-btn {
  width: 34px;
  height: 34px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
  position: relative;
  
  svg {
    width: 16px;
    height: 16px;
  }
  
  &.analyze-btn {
    background: linear-gradient(135deg, #8b5cf6, #6366f1);
    color: var(--text-inverse);
    
    .ai-pulse {
      position: absolute;
      width: 100%;
      height: 100%;
      border-radius: 8px;
      background: inherit;
      animation: aiPulse 2s ease-in-out infinite;
      z-index: -1;
    }
    
    &:hover {
      transform: scale(1.1);
      box-shadow: 0 4px 12px rgba(139, 92, 246, 0.4);
    }
  }
  
  &.view-btn {
    background: var(--bg-tertiary);
    color: var(--text-secondary);
    
    &:hover {
      background: var(--primary-500);
      color: var(--text-inverse);
    }
  }
  
  &.delete-btn {
    background: var(--bg-tertiary);
    color: var(--text-tertiary);
    
    &:hover {
      background: rgba($color-up, 0.1);
      color: $color-up;
    }
  }
}

@keyframes aiPulse {
  0%, 100% {
    transform: scale(1);
    opacity: 0;
  }
  50% {
    transform: scale(1.3);
    opacity: 0.3;
  }
}

// ==================== 空状态 ====================

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 80px 24px;
  background: var(--bg-card);
  border-radius: 14px;
  border: 1px solid var(--border-light);
  text-align: center;
  
  .empty-icon {
    width: 80px;
    height: 80px;
    border-radius: 24px;
    background: var(--warning-bg);
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 24px;
    
    svg {
      width: 40px;
      height: 40px;
      color: var(--warning);
    }
  }
  
  h3 {
    font-size: 20px;
    font-weight: 600;
    color: var(--text-primary);
    margin: 0 0 8px;
  }
  
  p {
    font-size: 14px;
    color: var(--text-tertiary);
    margin: 0 0 28px;
  }
}

.empty-add-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 14px 28px;
  background: var(--primary-500);
  border: none;
  border-radius: 12px;
  color: var(--text-inverse);
  font-size: 15px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  
  svg {
    width: 18px;
    height: 18px;
  }
  
  &:hover {
    background: var(--primary-600);
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(59, 130, 246, 0.35);
  }
}

// ==================== 弹窗样式 ====================

:deep(.add-dialog) {
  .el-dialog {
    border-radius: 16px;
    overflow: hidden;
  }
  
  .el-dialog__header {
    padding: 20px 24px;
    border-bottom: 1px solid var(--border-light);
    margin: 0;
  }
  
  .el-dialog__title {
    font-size: 18px;
    font-weight: 600;
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
  .search-section {
    margin-bottom: 20px;
    
    label {
      display: block;
      font-size: 14px;
      font-weight: 500;
      color: var(--text-primary);
      margin-bottom: 10px;
    }
  }
}

.dialog-search {
  width: 100%;
  
  :deep(.el-input__wrapper) {
    padding: 12px 16px;
    border-radius: 10px;
    box-shadow: none;
    border: 1px solid var(--border-color);
    
    &:focus-within {
      border-color: $color-primary;
      box-shadow: 0 0 0 3px rgba($color-primary, 0.1);
    }
  }
}

.suggestion-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 0;
}

.suggestion-main {
  display: flex;
  align-items: center;
  gap: 8px;
}

.suggestion-name {
  font-weight: 600;
  color: var(--text-primary);
}

.suggestion-industry {
  font-size: 12px;
  color: var(--text-tertiary);
  background: var(--bg-tertiary);
  padding: 2px 8px;
  border-radius: 4px;
}

.suggestion-code {
  font-size: 13px;
  font-family: var(--font-mono);
  color: var(--text-tertiary);
}

.selected-preview {
  padding: 16px;
  background: linear-gradient(135deg, rgba($color-primary, 0.05), rgba($color-primary, 0.02));
  border: 1px solid rgba($color-primary, 0.2);
  border-radius: 12px;
}

.preview-header {
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 8px;
}

.preview-name {
  font-size: 18px;
  font-weight: 700;
  color: var(--text-primary);
}

.preview-code {
  font-size: 14px;
  font-family: var(--font-mono);
  color: $color-primary;
}

.preview-meta {
  display: flex;
  gap: 8px;
}

.meta-tag {
  font-size: 12px;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  padding: 4px 10px;
  border-radius: 6px;
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
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 10px 24px;
  background: var(--primary-500);
  border: none;
  border-radius: 10px;
  font-size: 14px;
  font-weight: 500;
  color: var(--text-inverse);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover:not(:disabled) {
    background: var(--primary-600);
  }
  
  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
  
  .spinning {
    animation: spin 1s linear infinite;
  }
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

// ==================== 响应式 ====================

@media (max-width: 1024px) {
  .watchlist-page {
    padding: 16px;
  }
  
  .toolbar {
    flex-direction: column;
    align-items: stretch;
    gap: 12px;
  }
  
  .toolbar-left {
    flex-wrap: wrap;
  }
  
  .search-box {
    min-width: 100%;
  }
  
  .col.col-amount {
    display: none;
  }
}

@media (max-width: 640px) {
  .list-header {
    display: none;
  }
  
  .list-row {
    flex-wrap: wrap;
    gap: 12px;
    padding: 16px;
  }
  
  .col {
    &.col-name { flex: 1; min-width: 120px; }
    &.col-price { min-width: auto; }
    &.col-change { min-width: auto; }
    &.col-vol { display: none; }
    &.col-actions { 
      width: 100%; 
      justify-content: flex-start;
      margin-top: 8px;
      padding-top: 12px;
      border-top: 1px solid var(--border-light);
    }
  }
  
  .action-buttons {
    opacity: 1;
    transform: none;
  }
}
</style>
