<template>
  <div class="backtest-terminal">
    <!-- 顶部标题栏 -->
    <div class="header-strip">
      <div class="title-section">
        <span class="title-icon">⚡</span>
        <h1 class="title">量化回测</h1>
        <!-- <span class="subtitle">Vectorized Backtest Engine</span> -->
      </div>
      <div class="header-actions">
        <button class="action-btn" @click="showHistory = !showHistory">
          <span class="icon">📋</span>
          历史记录
        </button>
      </div>
    </div>

    <div class="main-content">
      <!-- 左侧: 参数配置 -->
      <div class="config-panel">
        <div class="panel-header">
          <span class="panel-icon">⚙️</span>
          <span>回测参数</span>
        </div>

        <div class="config-form">
          <!-- 股票选择 -->
          <div class="form-group">
            <label class="form-label">股票代码</label>
            <div class="stock-search-wrapper">
              <div class="search-input-box">
                <span class="search-icon">🔍</span>
                <input 
                  v-model="searchKeyword"
                  type="text"
                  class="search-input"
                  placeholder="输入代码或名称，如 600580 或 卧龙电驱"
                  @input="handleSearchInput"
                  @focus="showSuggestions = true"
                  @blur="handleSearchBlur"
                />
                <span v-if="stockName" class="selected-stock">
                  <span class="stock-code">{{ form.ts_code }}</span>
                  <span class="stock-name-tag">{{ stockName }}</span>
                </span>
              </div>
              <!-- 搜索建议列表 -->
              <div v-if="showSuggestions && suggestions.length > 0" class="suggestions-dropdown">
                <div 
                  v-for="stock in suggestions" 
                  :key="stock.ts_code"
                  class="suggestion-item"
                  @mousedown.prevent="selectStock(stock)"
                >
                  <span class="suggestion-code">{{ stock.ts_code }}</span>
                  <span class="suggestion-name">{{ stock.name }}</span>
                  <span class="suggestion-industry">{{ stock.industry || '' }}</span>
                </div>
              </div>
              <!-- 搜索中提示 -->
              <div v-if="showSuggestions && isSearching" class="suggestions-dropdown">
                <div class="suggestion-loading">
                  <span class="loading-spinner"></span>
                  搜索中...
                </div>
              </div>
              <!-- 无结果提示 -->
              <div v-if="showSuggestions && !isSearching && searchKeyword && suggestions.length === 0" class="suggestions-dropdown">
                <div class="suggestion-empty">未找到匹配的股票</div>
              </div>
            </div>
          </div>

          <!-- 日期范围 -->
          <div class="form-row">
            <div class="form-group">
              <label class="form-label">开始日期</label>
              <input 
                v-model="form.start_date"
                type="date"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label class="form-label">结束日期</label>
              <input 
                v-model="form.end_date"
                type="date"
                class="form-input"
              />
            </div>
          </div>

          <!-- 资金配置 -->
          <div class="form-group">
            <label class="form-label">初始资金</label>
            <div class="input-with-suffix">
              <input 
                v-model.number="form.initial_cash"
                type="number"
                class="form-input"
                min="10000"
                max="100000000"
              />
              <span class="input-suffix">元</span>
            </div>
          </div>

          <!-- 信号阈值 -->
          <div class="threshold-section">
            <div class="section-label">
              <span class="section-icon">🎯</span>
              信号阈值设置
            </div>
            <div class="threshold-cards">
              <div class="threshold-card buy">
                <div class="threshold-header">
                  <span class="threshold-icon">📈</span>
                  <span class="threshold-title">买入阈值</span>
                </div>
                <div class="threshold-desc">综合得分 &gt; 此值时买入</div>
                <div class="threshold-slider-wrap">
                  <input 
                    v-model.number="form.entry_threshold"
                    type="range"
                    class="threshold-slider buy-slider"
                    min="0.5"
                    max="0.95"
                    step="0.05"
                    :style="{ '--value': ((form.entry_threshold - 0.5) / 0.45 * 100) + '%' }"
                  />
                </div>
                <div class="threshold-value-display">
                  <span class="threshold-value">{{ (form.entry_threshold * 100).toFixed(0) }}</span>
                  <span class="threshold-unit">%</span>
                </div>
              </div>
              <div class="threshold-card sell">
                <div class="threshold-header">
                  <span class="threshold-icon">📉</span>
                  <span class="threshold-title">卖出阈值</span>
                </div>
                <div class="threshold-desc">综合得分 &lt; 此值时卖出</div>
                <div class="threshold-slider-wrap">
                  <input 
                    v-model.number="form.exit_threshold"
                    type="range"
                    class="threshold-slider sell-slider"
                    min="0.05"
                    max="0.5"
                    step="0.05"
                    :style="{ '--value': ((form.exit_threshold - 0.05) / 0.45 * 100) + '%' }"
                  />
                </div>
                <div class="threshold-value-display">
                  <span class="threshold-value">{{ (form.exit_threshold * 100).toFixed(0) }}</span>
                  <span class="threshold-unit">%</span>
                </div>
              </div>
            </div>
          </div>

          <!-- 仓位比例 -->
          <div class="position-section">
            <div class="section-label">
              <span class="section-icon">💰</span>
              仓位比例
            </div>
            <div class="position-card">
              <div class="position-slider-wrap">
                <div class="position-track">
                  <div class="position-fill" :style="{ width: (form.position_size * 100) + '%' }"></div>
                </div>
                <input 
                  v-model.number="form.position_size"
                  type="range"
                  class="position-slider"
                  min="0.1"
                  max="1"
                  step="0.1"
                />
              </div>
              <div class="position-labels">
                <span>10%</span>
                <span class="position-current">{{ (form.position_size * 100).toFixed(0) }}%</span>
                <span>100%</span>
              </div>
            </div>
          </div>

          <!-- 因子权重 -->
          <div class="factor-section">
            <div class="section-label">
              <span class="section-icon">⚖️</span>
              因子权重配置
              <span class="section-hint">拖动调整各因子对综合得分的影响</span>
            </div>
            <div class="factor-weights">
              <div v-for="(weight, factor) in form.factor_weights" :key="factor" class="factor-item">
                <div class="factor-info">
                  <span class="factor-icon">{{ getFactorIcon(factor) }}</span>
                  <span class="factor-name">{{ getFactorDisplayName(factor) }}</span>
                </div>
                <div class="factor-slider-wrap">
                  <input 
                    v-model.number="form.factor_weights[factor]"
                    type="range"
                    class="factor-slider"
                    min="0"
                    max="1"
                    step="0.05"
                    :style="{ '--value': (weight * 100) + '%' }"
                  />
                </div>
                <div class="factor-value">{{ (weight * 100).toFixed(0) }}%</div>
              </div>
            </div>
          </div>

          <!-- 验证提示 -->
          <div v-if="!isFormValid" class="validation-hint">
            <span class="hint-icon">⚠️</span>
            <span v-if="!isStockCodeValid">请输入正确的股票代码（如 000001.SZ）</span>
            <span v-else-if="!form.start_date || !form.end_date">请选择回测日期范围</span>
            <span v-else-if="form.initial_cash < 10000">初始资金不能少于 10000 元</span>
          </div>

          <!-- 提交按钮 -->
          <button 
            class="submit-btn"
            :disabled="isSubmitting || !isFormValid"
            @click="submitBacktest"
          >
            <span v-if="isSubmitting" class="btn-spinner"></span>
            <span v-else class="btn-icon">▶</span>
            {{ isSubmitting ? '提交中...' : '开始回测' }}
          </button>
        </div>
      </div>

      <!-- 右侧: 结果展示 -->
      <div class="result-panel">
        <!-- 任务状态 -->
        <div v-if="currentTask" class="task-status-card">
          <div class="status-header">
            <span class="status-icon" :class="currentTask.status">
              {{ getStatusIcon(currentTask.status) }}
            </span>
            <span class="status-text">{{ getStatusText(currentTask.status) }}</span>
            <span class="task-id">{{ currentTask.task_id }}</span>
          </div>
          <div v-if="currentTask.status === 'running'" class="progress-bar">
            <div class="progress-fill"></div>
          </div>
        </div>

        <!-- 结果展示 -->
        <template v-if="result && result.result">
          <!-- 核心指标卡片 -->
          <div class="metrics-grid">
            <div class="metric-card" :class="result.result.metrics.returns.total_return_pct >= 0 ? 'up' : 'down'">
              <span class="metric-label">总收益率</span>
              <span class="metric-value">{{ result.result.metrics.returns.total_return_pct.toFixed(2) }}%</span>
            </div>
            <div class="metric-card">
              <span class="metric-label">年化收益</span>
              <span class="metric-value">{{ result.result.metrics.returns.annual_return_pct.toFixed(2) }}%</span>
            </div>
            <div class="metric-card">
              <span class="metric-label">夏普比率</span>
              <span class="metric-value">{{ result.result.metrics.risk.sharpe_ratio.toFixed(2) }}</span>
            </div>
            <div class="metric-card warning">
              <span class="metric-label">最大回撤</span>
              <span class="metric-value">{{ result.result.metrics.risk.max_drawdown_pct.toFixed(2) }}%</span>
            </div>
            <div class="metric-card">
              <span class="metric-label">胜率</span>
              <span class="metric-value">{{ result.result.metrics.trades.win_rate_pct.toFixed(1) }}%</span>
            </div>
            <div class="metric-card">
              <span class="metric-label">盈亏比</span>
              <span class="metric-value">{{ result.result.metrics.trades.profit_factor.toFixed(2) }}</span>
            </div>
          </div>

          <!-- 收益曲线图表 -->
          <div class="chart-section">
            <div class="section-header">
              <span class="section-title">📈 收益曲线</span>
              <div class="chart-legend">
                <span class="legend-item strategy">策略</span>
                <span class="legend-item benchmark">基准</span>
              </div>
            </div>
            <div class="chart-container" ref="navChartRef"></div>
          </div>

          <!-- 回撤曲线 -->
          <div class="chart-section">
            <div class="section-header">
              <span class="section-title">📉 回撤曲线</span>
            </div>
            <div class="chart-container small" ref="drawdownChartRef"></div>
          </div>

          <!-- 详细指标 -->
          <div class="detail-section">
            <div class="section-header">
              <span class="section-title">📊 详细指标</span>
            </div>
            <div class="detail-grid">
              <div class="detail-group">
                <h4>收益指标</h4>
                <div class="detail-row">
                  <span>基准收益</span>
                  <span>{{ result.result.metrics.returns.benchmark_return_pct.toFixed(2) }}%</span>
                </div>
                <div class="detail-row">
                  <span>超额收益 (Alpha)</span>
                  <span :class="result.result.metrics.returns.alpha_pct >= 0 ? 'up' : 'down'">
                    {{ result.result.metrics.returns.alpha_pct.toFixed(2) }}%
                  </span>
                </div>
              </div>
              <div class="detail-group">
                <h4>风险指标</h4>
                <div class="detail-row">
                  <span>年化波动率</span>
                  <span>{{ result.result.metrics.risk.volatility_pct.toFixed(2) }}%</span>
                </div>
                <div class="detail-row">
                  <span>索提诺比率</span>
                  <span>{{ result.result.metrics.risk.sortino_ratio.toFixed(2) }}</span>
                </div>
                <div class="detail-row">
                  <span>卡玛比率</span>
                  <span>{{ result.result.metrics.risk.calmar_ratio.toFixed(2) }}</span>
                </div>
              </div>
              <div class="detail-group">
                <h4>交易统计</h4>
                <div class="detail-row">
                  <span>总交易次数</span>
                  <span>{{ result.result.metrics.trades.total_trades }}</span>
                </div>
                <div class="detail-row">
                  <span>平均盈利</span>
                  <span class="up">¥{{ result.result.metrics.trades.avg_profit.toFixed(2) }}</span>
                </div>
                <div class="detail-row">
                  <span>平均亏损</span>
                  <span class="down">¥{{ result.result.metrics.trades.avg_loss.toFixed(2) }}</span>
                </div>
                <div class="detail-row">
                  <span>最大连赢</span>
                  <span>{{ result.result.metrics.trades.max_consecutive_wins }}次</span>
                </div>
                <div class="detail-row">
                  <span>最大连亏</span>
                  <span>{{ result.result.metrics.trades.max_consecutive_losses }}次</span>
                </div>
              </div>
              <div class="detail-group">
                <h4>持仓统计</h4>
                <div class="detail-row">
                  <span>总交易日</span>
                  <span>{{ result.result.metrics.exposure.total_days }}天</span>
                </div>
                <div class="detail-row">
                  <span>持仓天数</span>
                  <span>{{ result.result.metrics.exposure.days_in_market }}天</span>
                </div>
                <div class="detail-row">
                  <span>仓位暴露</span>
                  <span>{{ result.result.metrics.exposure.market_exposure_pct.toFixed(1) }}%</span>
                </div>
                <div class="detail-row">
                  <span>平均持仓</span>
                  <span>{{ result.result.metrics.exposure.avg_holding_days.toFixed(1) }}天</span>
                </div>
              </div>
            </div>
          </div>

          <!-- 交易记录 -->
          <div class="trades-section">
            <div class="section-header">
              <span class="section-title">📝 交易记录</span>
              <span class="trades-count">共 {{ result.result.trades.length }} 笔</span>
            </div>
            <div class="trades-table-wrapper">
              <table class="trades-table">
                <thead>
                  <tr>
                    <th>日期</th>
                    <th>方向</th>
                    <th>价格</th>
                    <th>数量</th>
                    <th>金额</th>
                    <th>手续费</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(trade, idx) in result.result.trades" :key="idx">
                    <td>{{ trade.date }}</td>
                    <td>
                      <span class="trade-direction" :class="trade.direction">
                        {{ trade.direction === 'buy' ? '买入' : '卖出' }}
                      </span>
                    </td>
                    <td>¥{{ trade.price.toFixed(2) }}</td>
                    <td>{{ trade.shares }}</td>
                    <td>¥{{ trade.amount.toFixed(2) }}</td>
                    <td>¥{{ (trade.commission + (trade.stamp_duty || 0)).toFixed(2) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </template>

        <!-- 空状态 -->
        <div v-else-if="!currentTask" class="empty-state">
          <div class="empty-icon">📊</div>
          <h3>开始您的回测</h3>
          <p>配置参数并点击"开始回测"，结果将在这里展示</p>
        </div>
      </div>
    </div>

    <!-- 历史记录抽屉 -->
    <div class="history-drawer" :class="{ open: showHistory }">
      <div class="drawer-header">
        <h3>回测历史</h3>
        <button class="close-btn" @click="showHistory = false">×</button>
      </div>
      <div class="history-list">
        <div 
          v-for="item in history" 
          :key="item.task_id" 
          class="history-item"
          @click="loadHistoryResult(item.task_id)"
        >
          <div class="history-main">
            <span class="history-code">{{ item.stock_name || item.ts_code }}</span>
            <span class="history-date">{{ item.start_date }} ~ {{ item.end_date }}</span>
          </div>
          <div class="history-metrics">
            <span :class="(item.total_return_pct || 0) >= 0 ? 'up' : 'down'">
              {{ (item.total_return_pct || 0).toFixed(2) }}%
            </span>
            <span class="history-sharpe">夏普: {{ (item.sharpe_ratio || 0).toFixed(2) }}</span>
          </div>
          <div class="history-time">{{ formatTime(item.created_at) }}</div>
        </div>
        <div v-if="history.length === 0" class="no-history">
          暂无回测记录
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, computed, onMounted, nextTick } from 'vue'
import * as echarts from 'echarts'
import { 
  submitBacktest as submitBacktestApi, 
  getBacktestStatus, 
  getBacktestResult,
  getBacktestHistory,
  type BacktestResult,
  type BacktestHistoryItem,
  type BacktestStatus,
} from '@/api/modules/backtest'
import { stockApi } from '@/api'
import { ElMessage } from 'element-plus'

// ==================== 状态 ====================

const form = reactive({
  ts_code: '',
  start_date: getDefaultStartDate(),
  end_date: getDefaultEndDate(),
  initial_cash: 100000,
  entry_threshold: 0.7,
  exit_threshold: 0.3,
  position_size: 1.0,
  factor_weights: {
    tech_rsi: 0.25,
    tech_macd_signal: 0.25,
    tech_price_position: 0.25,
    tech_vol_ma5: 0.25,
  } as Record<string, number>,
})

const stockName = ref('')
const isSubmitting = ref(false)
const currentTask = ref<BacktestStatus | null>(null)
const result = ref<BacktestResult | null>(null)
const history = ref<BacktestHistoryItem[]>([])
const showHistory = ref(false)

// 股票搜索相关
const searchKeyword = ref('')
const suggestions = ref<Array<{ ts_code: string; name: string; industry?: string }>>([])
const showSuggestions = ref(false)
const isSearching = ref(false)
let searchTimeout: ReturnType<typeof setTimeout> | null = null

const navChartRef = ref<HTMLElement | null>(null)
const drawdownChartRef = ref<HTMLElement | null>(null)
let navChart: echarts.ECharts | null = null
let drawdownChart: echarts.ECharts | null = null

// ==================== 计算属性 ====================

const isStockCodeValid = computed(() => {
  return form.ts_code.match(/^\d{6}\.(SH|SZ|BJ)$/)
})

const isFormValid = computed(() => {
  return (
    isStockCodeValid.value &&
    form.start_date &&
    form.end_date &&
    form.initial_cash >= 10000
  )
})

// ==================== 方法 ====================

function getDefaultStartDate(): string {
  const date = new Date()
  date.setFullYear(date.getFullYear() - 1)
  return date.toISOString().split('T')[0]
}

function getDefaultEndDate(): string {
  return new Date().toISOString().split('T')[0]
}

function getFactorDisplayName(factor: string): string {
  const names: Record<string, string> = {
    tech_rsi: 'RSI指标',
    tech_macd_signal: 'MACD信号',
    tech_price_position: '价格位置',
    tech_vol_ma5: '成交量能',
  }
  return names[factor] || factor
}

function getFactorIcon(factor: string): string {
  const icons: Record<string, string> = {
    tech_rsi: '📊',
    tech_macd_signal: '📈',
    tech_price_position: '📍',
    tech_vol_ma5: '📶',
  }
  return icons[factor] || '📌'
}

function getStatusIcon(status: string): string {
  const icons: Record<string, string> = {
    pending: '⏳',
    queued: '📋',
    running: '⚡',
    completed: '✅',
    failed: '❌',
    cancelled: '🚫',
  }
  return icons[status] || '❓'
}

function getStatusText(status: string): string {
  const texts: Record<string, string> = {
    pending: '等待中',
    queued: '队列中',
    running: '执行中',
    completed: '已完成',
    failed: '失败',
    cancelled: '已取消',
  }
  return texts[status] || status
}

function formatTime(dateStr: string): string {
  if (!dateStr) return ''
  const date = new Date(dateStr)
  return date.toLocaleString('zh-CN', { 
    month: '2-digit', 
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

// 自动补全股票代码后缀
function autoCompleteTsCode(code: string): string {
  const num = code.replace(/\D/g, '')
  if (num.length !== 6) return ''
  
  // 根据股票代码前缀判断交易所
  if (num.startsWith('6')) {
    return `${num}.SH` // 上海
  } else if (num.startsWith('0') || num.startsWith('3')) {
    return `${num}.SZ` // 深圳
  } else if (num.startsWith('8') || num.startsWith('4')) {
    return `${num}.BJ` // 北交所
  }
  return ''
}

// 处理搜索输入
function handleSearchInput(): void {
  const keyword = searchKeyword.value.trim()
  
  // 清除之前的定时器
  if (searchTimeout) {
    clearTimeout(searchTimeout)
  }
  
  // 如果输入是 6 位数字，尝试自动补全
  if (/^\d{6}$/.test(keyword)) {
    const tsCode = autoCompleteTsCode(keyword)
    if (tsCode) {
      // 延迟搜索验证
      searchTimeout = setTimeout(async () => {
        try {
          isSearching.value = true
          const info = await stockApi.getStockBasic(tsCode)
          if (info) {
            form.ts_code = tsCode
            stockName.value = info.name
            searchKeyword.value = `${tsCode} ${info.name}`
            suggestions.value = []
            showSuggestions.value = false
          }
        } catch {
          // 如果找不到，继续搜索
          searchStocks(keyword)
        } finally {
          isSearching.value = false
        }
      }, 300)
      return
    }
  }
  
  // 普通搜索
  if (keyword.length >= 2) {
    searchTimeout = setTimeout(() => {
      searchStocks(keyword)
    }, 300)
  } else {
    suggestions.value = []
  }
}

// 搜索股票
async function searchStocks(keyword: string): Promise<void> {
  if (!keyword) return
  
  isSearching.value = true
  try {
    const results = await stockApi.searchStocks(keyword, 10)
    suggestions.value = results.map(s => ({
      ts_code: s.ts_code,
      name: s.name,
      industry: s.industry,
    }))
  } catch (error) {
    console.error('Search error:', error)
    suggestions.value = []
  } finally {
    isSearching.value = false
  }
}

// 选择股票
function selectStock(stock: { ts_code: string; name: string }): void {
  form.ts_code = stock.ts_code
  stockName.value = stock.name
  searchKeyword.value = `${stock.ts_code} ${stock.name}`
  suggestions.value = []
  showSuggestions.value = false
}

// 处理失焦
function handleSearchBlur(): void {
  // 延迟关闭，允许点击建议项
  setTimeout(() => {
    showSuggestions.value = false
  }, 200)
}

async function submitBacktest(): Promise<void> {
  if (!isFormValid.value) return
  
  isSubmitting.value = true
  result.value = null
  
  try {
    const response = await submitBacktestApi({
      ts_code: form.ts_code,
      stock_name: stockName.value || undefined,
      start_date: form.start_date.replace(/-/g, ''),
      end_date: form.end_date.replace(/-/g, ''),
      initial_cash: form.initial_cash,
      entry_threshold: form.entry_threshold,
      exit_threshold: form.exit_threshold,
      position_size: form.position_size,
      factor_weights: form.factor_weights,
      auto_technical: true,
    })
    
    currentTask.value = {
      task_id: response.task_id,
      status: response.status as any,
    }
    
    ElMessage.success('回测任务已提交')
    
    // 开始轮询状态
    pollTaskStatus(response.task_id)
    
  } catch (error: any) {
    ElMessage.error(error.message || '提交失败')
  } finally {
    isSubmitting.value = false
  }
}

async function pollTaskStatus(taskId: string): Promise<void> {
  const maxAttempts = 120 // 最多轮询 2 分钟
  let attempts = 0
  
  const poll = async () => {
    if (attempts >= maxAttempts) {
      ElMessage.warning('回测超时，请稍后查看结果')
      return
    }
    
    try {
      const status = await getBacktestStatus(taskId)
      currentTask.value = status
      
      if (status.status === 'completed') {
        // 获取结果
        const res = await getBacktestResult(taskId)
        result.value = res
        
        // 渲染图表
        await nextTick()
        renderCharts()
        
        // 刷新历史
        loadHistory()
        
        ElMessage.success('回测完成！')
        
      } else if (status.status === 'failed') {
        ElMessage.error(status.error || '回测失败')
        
      } else if (status.status === 'running' || status.status === 'queued' || status.status === 'pending') {
        // 继续轮询
        attempts++
        setTimeout(poll, 1000)
      }
      
    } catch (error) {
      console.error('Poll error:', error)
      attempts++
      setTimeout(poll, 2000)
    }
  }
  
  poll()
}

async function loadHistory(): Promise<void> {
  try {
    const res = await getBacktestHistory(20, 0)
    history.value = res.items
  } catch (error) {
    console.error('Load history error:', error)
  }
}

async function loadHistoryResult(taskId: string): Promise<void> {
  try {
    const res = await getBacktestResult(taskId)
    result.value = res
    currentTask.value = { task_id: taskId, status: 'completed' }
    showHistory.value = false
    
    await nextTick()
    renderCharts()
    
  } catch (error: any) {
    ElMessage.error(error.message || '加载失败')
  }
}

function renderCharts(): void {
  if (!result.value?.result?.charts) return
  
  const charts = result.value.result.charts
  
  // 收益曲线
  if (navChartRef.value) {
    if (navChart) navChart.dispose()
    navChart = echarts.init(navChartRef.value)
    
    navChart.setOption({
      backgroundColor: 'transparent',
      grid: { left: 50, right: 20, top: 20, bottom: 30 },
      xAxis: {
        type: 'category',
        data: charts.nav_series.dates,
        axisLine: { lineStyle: { color: 'var(--border-color)' } },
        axisLabel: { color: 'var(--text-secondary)' },
      },
      yAxis: {
        type: 'value',
        axisLine: { show: false },
        axisLabel: { color: 'var(--text-secondary)' },
        splitLine: { lineStyle: { color: 'var(--border-color)', opacity: 0.3 } },
      },
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'var(--bg-elevated)',
        borderColor: 'var(--border-color)',
        textStyle: { color: 'var(--text-primary)' },
      },
      series: [
        {
          name: '策略',
          type: 'line',
          data: charts.nav_series.strategy,
          smooth: true,
          lineStyle: { color: '#ef4444', width: 2 },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: 'rgba(239, 68, 68, 0.3)' },
              { offset: 1, color: 'rgba(239, 68, 68, 0)' },
            ]),
          },
          symbol: 'none',
        },
        {
          name: '基准',
          type: 'line',
          data: charts.nav_series.benchmark,
          smooth: true,
          lineStyle: { color: '#6b7280', width: 1, type: 'dashed' },
          symbol: 'none',
        },
      ],
    })
  }
  
  // 回撤曲线
  if (drawdownChartRef.value) {
    if (drawdownChart) drawdownChart.dispose()
    drawdownChart = echarts.init(drawdownChartRef.value)
    
    drawdownChart.setOption({
      backgroundColor: 'transparent',
      grid: { left: 50, right: 20, top: 10, bottom: 30 },
      xAxis: {
        type: 'category',
        data: charts.drawdown_series.dates,
        axisLine: { lineStyle: { color: 'var(--border-color)' } },
        axisLabel: { color: 'var(--text-secondary)', show: false },
      },
      yAxis: {
        type: 'value',
        axisLine: { show: false },
        axisLabel: { color: 'var(--text-secondary)' },
        splitLine: { lineStyle: { color: 'var(--border-color)', opacity: 0.3 } },
      },
      series: [
        {
          type: 'line',
          data: charts.drawdown_series.values,
          smooth: true,
          lineStyle: { color: '#ef4444', width: 1 },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: 'rgba(239, 68, 68, 0)' },
              { offset: 1, color: 'rgba(239, 68, 68, 0.3)' },
            ]),
          },
          symbol: 'none',
        },
      ],
    })
  }
}

// ==================== 生命周期 ====================

onMounted(() => {
  loadHistory()
})

// 监听窗口变化
window.addEventListener('resize', () => {
  navChart?.resize()
  drawdownChart?.resize()
})
</script>

<style scoped lang="scss">
.backtest-terminal {
  min-height: 100vh;
  background: var(--bg-primary);
  color: var(--text-primary);
  padding: 20px;
}

// 顶部标题栏
.header-strip {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 24px;
  background: var(--bg-elevated);
  border-radius: 12px;
  margin-bottom: 20px;
  border: 1px solid var(--border-color);
}

.title-section {
  display: flex;
  align-items: center;
  gap: 12px;
}

.title-icon {
  font-size: 24px;
}

.title {
  font-size: 20px;
  font-weight: 600;
  margin: 0;
}

.subtitle {
  color: var(--text-secondary);
  font-size: 12px;
  font-family: 'JetBrains Mono', monospace;
}

.action-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  color: var(--text-primary);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-hover);
    border-color: var(--accent-color);
  }
}

// 主内容区
.main-content {
  display: grid;
  grid-template-columns: 360px 1fr;
  gap: 20px;
}

// 配置面板
.config-panel {
  background: var(--bg-elevated);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.panel-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 16px 20px;
  background: var(--bg-secondary);
  border-bottom: 1px solid var(--border-color);
  font-weight: 600;
}

.config-form {
  padding: 20px;
}

.form-group {
  margin-bottom: 20px;
}

.form-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}

.form-label {
  display: block;
  margin-bottom: 8px;
  font-size: 13px;
  color: var(--text-secondary);
}

.label-hint {
  font-size: 11px;
  color: var(--text-muted);
  margin-left: 4px;
}

.form-input {
  width: 100%;
  padding: 10px 12px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  color: var(--text-primary);
  font-size: 14px;
  transition: all 0.2s;
  
  &:focus {
    outline: none;
    border-color: var(--accent-color);
    box-shadow: 0 0 0 3px rgba(var(--accent-rgb), 0.1);
  }
}

// ==================== 股票搜索组件 ====================
.stock-search-wrapper {
  position: relative;
}

.search-input-box {
  position: relative;
  display: flex;
  align-items: center;
}

.search-icon {
  position: absolute;
  left: 12px;
  font-size: 14px;
  z-index: 1;
}

.search-input {
  width: 100%;
  padding: 12px 12px 12px 38px;
  background: var(--bg-secondary);
  border: 2px solid var(--border-color);
  border-radius: 10px;
  color: var(--text-primary);
  font-size: 14px;
  transition: all 0.2s;
  
  &::placeholder {
    color: var(--text-muted);
    font-size: 12px;
  }
  
  &:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.15);
  }
}

.selected-stock {
  position: absolute;
  right: 12px;
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  background: linear-gradient(135deg, rgba(16, 185, 129, 0.1), rgba(59, 130, 246, 0.1));
  border: 1px solid rgba(16, 185, 129, 0.3);
  border-radius: 6px;
}

.stock-code {
  font-size: 11px;
  font-weight: 600;
  color: #3b82f6;
  font-family: 'JetBrains Mono', monospace;
}

.stock-name-tag {
  font-size: 11px;
  color: #10b981;
}

// 搜索建议下拉框
.suggestions-dropdown {
  position: absolute;
  top: 100%;
  left: 0;
  right: 0;
  margin-top: 4px;
  background: var(--bg-elevated);
  border: 1px solid var(--border-color);
  border-radius: 10px;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.2);
  z-index: 100;
  max-height: 280px;
  overflow-y: auto;
}

.suggestion-item {
  display: grid;
  grid-template-columns: 90px 1fr auto;
  align-items: center;
  gap: 12px;
  padding: 12px 14px;
  cursor: pointer;
  transition: all 0.15s;
  border-bottom: 1px solid var(--border-color);
  
  &:last-child {
    border-bottom: none;
  }
  
  &:hover {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.1), rgba(139, 92, 246, 0.05));
  }
}

.suggestion-code {
  font-size: 13px;
  font-weight: 600;
  color: #3b82f6;
  font-family: 'JetBrains Mono', monospace;
}

.suggestion-name {
  font-size: 13px;
  color: var(--text-primary);
  font-weight: 500;
}

.suggestion-industry {
  font-size: 11px;
  color: var(--text-muted);
  padding: 2px 8px;
  background: var(--bg-secondary);
  border-radius: 4px;
}

.suggestion-loading,
.suggestion-empty {
  padding: 20px;
  text-align: center;
  color: var(--text-muted);
  font-size: 13px;
}

.loading-spinner {
  display: inline-block;
  width: 14px;
  height: 14px;
  border: 2px solid var(--border-color);
  border-top-color: #3b82f6;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  margin-right: 8px;
  vertical-align: middle;
}

.stock-input-wrapper {
  position: relative;
}

.stock-name {
  position: absolute;
  right: 12px;
  top: 50%;
  transform: translateY(-50%);
  color: var(--accent-color);
  font-size: 12px;
}

.input-with-suffix {
  position: relative;
}

.input-suffix {
  position: absolute;
  right: 12px;
  top: 50%;
  transform: translateY(-50%);
  color: var(--text-muted);
  font-size: 12px;
}

// 通用区域标签
.section-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 14px;
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 12px;
}

.section-icon {
  font-size: 16px;
}

.section-hint {
  font-size: 11px;
  font-weight: 400;
  color: var(--text-muted);
  margin-left: auto;
}

// ==================== 阈值设置区域 ====================
.threshold-section {
  margin-bottom: 24px;
}

.threshold-cards {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}

.threshold-card {
  padding: 16px;
  border-radius: 12px;
  border: 2px solid transparent;
  transition: all 0.3s;
  
  &.buy {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.08), rgba(16, 185, 129, 0.02));
    border-color: rgba(16, 185, 129, 0.3);
    
    &:hover {
      border-color: rgba(16, 185, 129, 0.5);
      box-shadow: 0 4px 20px rgba(16, 185, 129, 0.15);
    }
    
    .threshold-value { color: #10b981; }
  }
  
  &.sell {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.08), rgba(239, 68, 68, 0.02));
    border-color: rgba(239, 68, 68, 0.3);
    
    &:hover {
      border-color: rgba(239, 68, 68, 0.5);
      box-shadow: 0 4px 20px rgba(239, 68, 68, 0.15);
    }
    
    .threshold-value { color: #ef4444; }
  }
}

.threshold-header {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 4px;
}

.threshold-icon {
  font-size: 16px;
}

.threshold-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
}

.threshold-desc {
  font-size: 11px;
  color: var(--text-muted);
  margin-bottom: 12px;
}

.threshold-slider-wrap {
  margin-bottom: 8px;
}

.threshold-slider {
  width: 100%;
  height: 8px;
  -webkit-appearance: none;
  appearance: none;
  border-radius: 4px;
  outline: none;
  cursor: pointer;
  
  &.buy-slider {
    background: linear-gradient(to right, #10b981 0%, #10b981 var(--value, 50%), #374151 var(--value, 50%), #374151 100%);
    
    &::-webkit-slider-thumb {
      -webkit-appearance: none;
      width: 20px;
      height: 20px;
      background: linear-gradient(135deg, #10b981, #059669);
      border: 3px solid white;
      border-radius: 50%;
      cursor: grab;
      box-shadow: 0 2px 8px rgba(16, 185, 129, 0.5);
      transition: transform 0.2s;
      
      &:hover { transform: scale(1.15); }
    }
  }
  
  &.sell-slider {
    background: linear-gradient(to right, #ef4444 0%, #ef4444 var(--value, 50%), #374151 var(--value, 50%), #374151 100%);
    
    &::-webkit-slider-thumb {
      -webkit-appearance: none;
      width: 20px;
      height: 20px;
      background: linear-gradient(135deg, #ef4444, #dc2626);
      border: 3px solid white;
      border-radius: 50%;
      cursor: grab;
      box-shadow: 0 2px 8px rgba(239, 68, 68, 0.5);
      transition: transform 0.2s;
      
      &:hover { transform: scale(1.15); }
    }
  }
}

.threshold-value-display {
  text-align: center;
}

.threshold-value {
  font-size: 28px;
  font-weight: 700;
  font-family: 'JetBrains Mono', monospace;
}

.threshold-unit {
  font-size: 14px;
  color: var(--text-muted);
}

// ==================== 仓位比例区域 ====================
.position-section {
  margin-bottom: 24px;
}

.position-card {
  padding: 20px;
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.08), rgba(139, 92, 246, 0.05));
  border: 1px solid var(--border-color);
  border-radius: 12px;
}

.position-slider-wrap {
  position: relative;
  height: 12px;
  margin-bottom: 12px;
}

.position-track {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 12px;
  background: #374151;
  border-radius: 6px;
  overflow: hidden;
}

.position-fill {
  height: 100%;
  background: linear-gradient(90deg, #3b82f6, #8b5cf6);
  border-radius: 6px;
  transition: width 0.2s;
}

.position-slider {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 12px;
  -webkit-appearance: none;
  appearance: none;
  background: transparent;
  cursor: pointer;
  
  &::-webkit-slider-thumb {
    -webkit-appearance: none;
    width: 24px;
    height: 24px;
    background: linear-gradient(135deg, #3b82f6, #8b5cf6);
    border: 3px solid white;
    border-radius: 50%;
    cursor: grab;
    box-shadow: 0 2px 10px rgba(59, 130, 246, 0.5);
    transition: transform 0.2s;
    
    &:hover { transform: scale(1.15); }
  }
}

.position-labels {
  display: flex;
  justify-content: space-between;
  font-size: 11px;
  color: var(--text-muted);
}

.position-current {
  font-size: 16px;
  font-weight: 700;
  color: #3b82f6;
  font-family: 'JetBrains Mono', monospace;
}

// ==================== 因子权重区域 ====================
.factor-section {
  margin-bottom: 24px;
}

.factor-weights {
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 16px;
  background: linear-gradient(135deg, rgba(139, 92, 246, 0.06), rgba(236, 72, 153, 0.04));
  border: 1px solid var(--border-color);
  border-radius: 12px;
}

.factor-item {
  display: grid;
  grid-template-columns: 100px 1fr 55px;
  align-items: center;
  gap: 12px;
  padding: 12px 14px;
  background: var(--bg-secondary);
  border-radius: 10px;
  border: 1px solid transparent;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-hover);
    border-color: rgba(139, 92, 246, 0.3);
    transform: translateX(3px);
  }
}

.factor-info {
  display: flex;
  align-items: center;
  gap: 8px;
}

.factor-icon {
  font-size: 16px;
}

.factor-name {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
}

.factor-slider-wrap {
  position: relative;
}

.factor-slider {
  width: 100%;
  height: 6px;
  -webkit-appearance: none;
  appearance: none;
  background: linear-gradient(to right, #8b5cf6 0%, #8b5cf6 var(--value, 25%), #4b5563 var(--value, 25%), #4b5563 100%);
  border-radius: 3px;
  cursor: pointer;
  
  &::-webkit-slider-thumb {
    -webkit-appearance: none;
    width: 18px;
    height: 18px;
    background: linear-gradient(135deg, #8b5cf6, #ec4899);
    border: 2px solid white;
    border-radius: 50%;
    cursor: grab;
    box-shadow: 0 2px 6px rgba(139, 92, 246, 0.4);
    transition: all 0.2s;
    
    &:hover {
      transform: scale(1.2);
      box-shadow: 0 4px 12px rgba(139, 92, 246, 0.5);
    }
  }
}

.factor-value {
  font-size: 14px;
  font-weight: 700;
  color: #8b5cf6;
  text-align: center;
  font-family: 'JetBrains Mono', monospace;
  padding: 4px 8px;
  background: rgba(139, 92, 246, 0.12);
  border-radius: 6px;
  min-width: 50px;
}

// 验证提示
.validation-hint {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 14px;
  margin-bottom: 12px;
  background: rgba(245, 158, 11, 0.1);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 8px;
  font-size: 13px;
  color: #f59e0b;
}

.hint-icon {
  font-size: 16px;
}

.submit-btn {
  width: 100%;
  padding: 14px;
  background: linear-gradient(135deg, #10b981, #059669);
  border: none;
  border-radius: 10px;
  color: white;
  font-size: 15px;
  font-weight: 600;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  transition: all 0.3s;
  
  &:hover:not(:disabled) {
    transform: translateY(-1px);
    box-shadow: 0 4px 20px rgba(16, 185, 129, 0.4);
  }
  
  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
}

.btn-spinner {
  width: 16px;
  height: 16px;
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-top-color: white;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

// 结果面板
.result-panel {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

// 任务状态卡片
.task-status-card {
  background: var(--bg-elevated);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  padding: 16px 20px;
}

.status-header {
  display: flex;
  align-items: center;
  gap: 10px;
}

.status-icon {
  font-size: 20px;
  
  &.running {
    animation: pulse 1.5s infinite;
  }
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}

.status-text {
  font-weight: 600;
}

.task-id {
  margin-left: auto;
  font-size: 11px;
  color: var(--text-muted);
  font-family: 'JetBrains Mono', monospace;
}

.progress-bar {
  margin-top: 12px;
  height: 4px;
  background: var(--bg-secondary);
  border-radius: 2px;
  overflow: hidden;
}

.progress-fill {
  height: 100%;
  width: 30%;
  background: linear-gradient(90deg, #10b981, #34d399);
  animation: progress-move 1.5s ease-in-out infinite;
}

@keyframes progress-move {
  0% { transform: translateX(-100%); }
  100% { transform: translateX(400%); }
}

// 指标网格
.metrics-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

.metric-card {
  background: var(--bg-elevated);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  padding: 20px;
  text-align: center;
  
  &.up {
    border-color: rgba(239, 68, 68, 0.3);
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.05), transparent);
    
    .metric-value { color: #ef4444; }
  }
  
  &.down {
    border-color: rgba(16, 185, 129, 0.3);
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.05), transparent);
    
    .metric-value { color: #10b981; }
  }
  
  &.warning {
    .metric-value { color: #f59e0b; }
  }
}

.metric-label {
  display: block;
  font-size: 12px;
  color: var(--text-secondary);
  margin-bottom: 8px;
}

.metric-value {
  font-size: 24px;
  font-weight: 700;
  font-family: 'JetBrains Mono', monospace;
}

// 图表区域
.chart-section {
  background: var(--bg-elevated);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  padding: 20px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.section-title {
  font-weight: 600;
}

.chart-legend {
  display: flex;
  gap: 16px;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--text-secondary);
  
  &::before {
    content: '';
    width: 12px;
    height: 3px;
    border-radius: 1.5px;
  }
  
  &.strategy::before {
    background: #ef4444;
  }
  
  &.benchmark::before {
    background: #6b7280;
  }
}

.chart-container {
  height: 300px;
  
  &.small {
    height: 150px;
  }
}

// 详细指标
.detail-section {
  background: var(--bg-elevated);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  padding: 20px;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 24px;
}

.detail-group {
  h4 {
    font-size: 13px;
    color: var(--text-secondary);
    margin: 0 0 12px 0;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border-color);
  }
}

.detail-row {
  display: flex;
  justify-content: space-between;
  padding: 6px 0;
  font-size: 13px;
  
  span:first-child {
    color: var(--text-secondary);
  }
  
  span:last-child {
    font-family: 'JetBrains Mono', monospace;
  }
  
  .up { color: #ef4444; }
  .down { color: #10b981; }
}

// 交易记录
.trades-section {
  background: var(--bg-elevated);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  padding: 20px;
}

.trades-count {
  font-size: 12px;
  color: var(--text-muted);
}

.trades-table-wrapper {
  max-height: 300px;
  overflow-y: auto;
  margin-top: 16px;
}

.trades-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
  
  th, td {
    padding: 10px 12px;
    text-align: left;
    border-bottom: 1px solid var(--border-color);
  }
  
  th {
    font-weight: 500;
    color: var(--text-secondary);
    background: var(--bg-secondary);
    position: sticky;
    top: 0;
  }
  
  td {
    font-family: 'JetBrains Mono', monospace;
  }
}

.trade-direction {
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 500;
  
  &.buy {
    background: rgba(239, 68, 68, 0.1);
    color: #ef4444;
  }
  
  &.sell {
    background: rgba(16, 185, 129, 0.1);
    color: #10b981;
  }
}

// 空状态
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 80px 40px;
  background: var(--bg-elevated);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  text-align: center;
}

.empty-icon {
  font-size: 64px;
  margin-bottom: 20px;
  opacity: 0.6;
}

.empty-state h3 {
  margin: 0 0 8px 0;
  color: var(--text-primary);
}

.empty-state p {
  margin: 0;
  color: var(--text-secondary);
  font-size: 14px;
}

// 历史抽屉
.history-drawer {
  position: fixed;
  top: 0;
  right: -400px;
  width: 400px;
  height: 100vh;
  background: var(--bg-elevated);
  border-left: 1px solid var(--border-color);
  transition: right 0.3s ease;
  z-index: 1000;
  display: flex;
  flex-direction: column;
  
  &.open {
    right: 0;
  }
}

.drawer-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px;
  border-bottom: 1px solid var(--border-color);
  
  h3 {
    margin: 0;
    font-size: 16px;
  }
}

.close-btn {
  width: 32px;
  height: 32px;
  background: var(--bg-secondary);
  border: none;
  border-radius: 8px;
  color: var(--text-primary);
  font-size: 20px;
  cursor: pointer;
  
  &:hover {
    background: var(--bg-hover);
  }
}

.history-list {
  flex: 1;
  overflow-y: auto;
  padding: 12px;
}

.history-item {
  padding: 16px;
  background: var(--bg-secondary);
  border-radius: 10px;
  margin-bottom: 10px;
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-hover);
    transform: translateX(-4px);
  }
}

.history-main {
  display: flex;
  justify-content: space-between;
  margin-bottom: 8px;
}

.history-code {
  font-weight: 600;
  font-family: 'JetBrains Mono', monospace;
}

.history-date {
  font-size: 11px;
  color: var(--text-muted);
}

.history-metrics {
  display: flex;
  gap: 16px;
  font-size: 14px;
  font-family: 'JetBrains Mono', monospace;
  
  .up { color: #ef4444; }
  .down { color: #10b981; }
}

.history-sharpe {
  color: var(--text-secondary);
  font-size: 12px;
}

.history-time {
  margin-top: 8px;
  font-size: 11px;
  color: var(--text-muted);
}

.no-history {
  text-align: center;
  padding: 40px;
  color: var(--text-muted);
}

// 响应式
@media (max-width: 1200px) {
  .main-content {
    grid-template-columns: 1fr;
  }
  
  .metrics-grid {
    grid-template-columns: repeat(2, 1fr);
  }
  
  .detail-grid {
    grid-template-columns: 1fr;
  }
}
</style>
