<script setup lang="ts">
/**
 * 分析任务列表页面
 * 
 * 全面 UI 升级版 - 专业金融分析工具风格
 */
import { ref, onMounted, computed } from 'vue'
import { useRouter } from 'vue-router'
import { useTaskStore } from '@/stores/task'
import { useTask } from '@/hooks'
import { taskApi, stockApi } from '@/api'
import { ElMessage, ElMessageBox, ElDialog, ElRadioGroup, ElRadioButton, ElAutocomplete } from 'element-plus'
import { 
  Plus, 
  Clock, 
  Check, 
  Close, 
  Loading,
  Timer,
  DataAnalysis,
  TrendCharts,
  Document,
  Search,
  Filter,
  ArrowRight,
  Delete,
} from '@element-plus/icons-vue'
import type { Task, StockBasic } from '@/api/types'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import 'dayjs/locale/zh-cn'

dayjs.extend(relativeTime)
dayjs.locale('zh-cn')

const router = useRouter()
const taskStore = useTaskStore()
const { refreshTaskList, isLoading, analyzeStock, analyzeMarket, isCreating } = useTask()

// 筛选条件
const statusFilter = ref<string>('all')
const typeFilter = ref<string>('all')

// 删除操作状态
const deletingTaskId = ref<string | null>(null)

// 新建任务弹窗
const createDialogVisible = ref(false)
const newTaskType = ref<'stock' | 'market'>('stock')
const newStockCode = ref('')
const selectedStock = ref<StockBasic | null>(null)

// 任务类型配置
const taskTypeConfig: Record<string, { label: string; icon: typeof DataAnalysis; color: string }> = {
  stock_analysis: { label: '个股分析', icon: DataAnalysis, color: '#3b82f6' },
  market_overview: { label: '大盘分析', icon: TrendCharts, color: '#8b5cf6' },
  strategy_backtest: { label: '策略回测', icon: Timer, color: '#06b6d4' },
  news_sentiment: { label: '舆情分析', icon: Document, color: '#f59e0b' },
  report_generate: { label: '研报生成', icon: Document, color: '#10b981' },
  custom_query: { label: '自定义查询', icon: Search, color: '#6366f1' },
}

// 状态配置
const statusConfig: Record<string, { label: string; icon: typeof Clock; color: string; bgColor: string; borderColor: string }> = {
  pending: { label: '等待中', icon: Clock, color: '#64748b', bgColor: 'rgba(100, 116, 139, 0.1)', borderColor: '#94a3b8' },
  queued: { label: '排队中', icon: Clock, color: '#64748b', bgColor: 'rgba(100, 116, 139, 0.1)', borderColor: '#94a3b8' },
  running: { label: '进行中', icon: Loading, color: '#f59e0b', bgColor: 'rgba(245, 158, 11, 0.1)', borderColor: '#fbbf24' },
  completed: { label: '已完成', icon: Check, color: '#22c55e', bgColor: 'rgba(34, 197, 94, 0.1)', borderColor: '#4ade80' },
  failed: { label: '失败', icon: Close, color: '#ef4444', bgColor: 'rgba(239, 68, 68, 0.1)', borderColor: '#f87171' },
  cancelled: { label: '已取消', icon: Close, color: '#64748b', bgColor: 'rgba(100, 116, 139, 0.1)', borderColor: '#94a3b8' },
}

// 信号配置 (A股：红涨绿跌)
const signalConfig: Record<string, { label: string; color: string; bgColor: string }> = {
  buy: { label: '买入', color: '#ef4444', bgColor: 'rgba(239, 68, 68, 0.1)' },
  sell: { label: '卖出', color: '#22c55e', bgColor: 'rgba(34, 197, 94, 0.1)' },
  hold: { label: '持有', color: '#f59e0b', bgColor: 'rgba(245, 158, 11, 0.1)' },
  strong_buy: { label: '强烈买入', color: '#dc2626', bgColor: 'rgba(220, 38, 38, 0.15)' },
  strong_sell: { label: '强烈卖出', color: '#16a34a', bgColor: 'rgba(22, 163, 74, 0.15)' },
}

// 筛选后的任务列表
const filteredTasks = computed(() => {
  let tasks = taskStore.tasks || []
  
  if (statusFilter.value && statusFilter.value !== 'all') {
    tasks = tasks.filter(t => t.status === statusFilter.value)
  }
  
  if (typeFilter.value && typeFilter.value !== 'all') {
    tasks = tasks.filter(t => t.task_type === typeFilter.value)
  }
  
  return tasks
})

// 加载数据
onMounted(async () => {
  await loadTasks()
})

async function loadTasks() {
  await refreshTaskList()
}

// 查看详情
function viewTask(taskId: string) {
  router.push(`/analysis/${taskId}`)
}

// 删除任务
async function handleDeleteTask(event: Event, task: Task) {
  event.stopPropagation() // 阻止冒泡到卡片点击
  
  // 检查任务状态
  const deletableStatuses = ['completed', 'failed', 'cancelled']
  if (!deletableStatuses.includes(task.status)) {
    ElMessage.warning('只能删除已完成、失败或已取消的任务')
    return
  }
  
  try {
    await ElMessageBox.confirm(
      '确定要删除这个任务吗？删除后无法恢复。',
      '删除确认',
      {
        confirmButtonText: '删除',
        cancelButtonText: '取消',
        type: 'warning',
        confirmButtonClass: 'el-button--danger',
      }
    )
    
    deletingTaskId.value = task.task_id
    
    await taskApi.deleteTask(task.task_id)
    taskStore.removeTask(task.task_id)
    
    ElMessage.success('任务已删除')
  } catch (error: unknown) {
    if (error !== 'cancel') {
      const errMsg = error instanceof Error ? error.message : '删除失败'
      ElMessage.error(errMsg)
    }
  } finally {
    deletingTaskId.value = null
  }
}

// 判断任务是否可删除
function isDeletable(task: Task): boolean {
  const deletableStatuses = ['completed', 'failed', 'cancelled']
  return deletableStatuses.includes(task.status)
}

// 格式化时间
function formatTime(time: string) {
  return dayjs(time).fromNow()
}

// 获取进度
function getProgress(taskId: string) {
  return taskStore.getProgress(taskId)
}

// 获取置信度颜色
function getConfidenceColor(confidence: number): string {
  if (confidence >= 0.8) return '#22c55e'
  if (confidence >= 0.6) return '#f59e0b'
  return '#ef4444'
}

function getTaskTypeConfig(type: string) {
  return taskTypeConfig[type] || { label: type, icon: Document, color: '#64748b' }
}

function getStatusConfig(status: string) {
  return statusConfig[status] || statusConfig.pending
}

function getSignalConfig(signal: string) {
  return signalConfig[signal] || { label: signal, color: '#64748b', bgColor: 'rgba(100, 116, 139, 0.1)' }
}

// 获取股票显示名称
function getStockDisplayName(task: Task, tsCode: string): string {
  const stockInfo = task.stock_names?.find(s => s.ts_code === tsCode)
  return stockInfo?.name || tsCode
}

// ==================== 新建任务 ====================

function openCreateDialog() {
  createDialogVisible.value = true
  newTaskType.value = 'stock'
  newStockCode.value = ''
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
  newStockCode.value = stock.ts_code || ''
}

async function handleCreateTask(): Promise<void> {
  if (newTaskType.value === 'stock') {
    if (!newStockCode.value.trim()) {
      ElMessage.warning('请输入股票代码')
      return
    }
    await analyzeStock(newStockCode.value.toUpperCase())
  } else {
    await analyzeMarket()
  }
  
  createDialogVisible.value = false
  newStockCode.value = ''
  selectedStock.value = null
  
  // 刷新列表
  await loadTasks()
}
</script>

<template>
  <div class="analysis-list-view">
    <!-- 页面头部 -->
    <header class="page-header">
      <div class="header-left">
        <h1 class="page-title">分析任务</h1>
        <span class="task-count">{{ filteredTasks.length }} 个任务</span>
      </div>
      <button class="create-btn" @click="openCreateDialog">
        <Plus />
        <span>新建任务</span>
      </button>
    </header>
    
    <!-- 新建任务弹窗 -->
    <ElDialog
      v-model="createDialogVisible"
      title="新建分析任务"
      width="480px"
      :close-on-click-modal="false"
      class="create-task-dialog"
    >
      <div class="dialog-content">
        <!-- 任务类型选择 -->
        <div class="form-item">
          <label class="form-label">任务类型</label>
          <ElRadioGroup v-model="newTaskType" class="task-type-group">
            <ElRadioButton value="stock">
              <DataAnalysis class="type-icon" />
              <span>个股分析</span>
            </ElRadioButton>
            <ElRadioButton value="market">
              <TrendCharts class="type-icon" />
              <span>大盘分析</span>
            </ElRadioButton>
          </ElRadioGroup>
        </div>
        
        <!-- 股票代码输入 (仅个股分析时显示) -->
        <div v-if="newTaskType === 'stock'" class="form-item">
          <label class="form-label">股票代码</label>
          <ElAutocomplete
            v-model="newStockCode"
            :fetch-suggestions="handleStockSearch"
            placeholder="输入股票代码或名称搜索"
            clearable
            class="stock-input"
            value-key="ts_code"
            :debounce="300"
            @select="handleStockSelect"
          >
            <template #default="{ item }">
              <div class="stock-suggestion">
                <span class="stock-name">{{ item.name }}</span>
                <span class="stock-code">{{ item.ts_code }}</span>
              </div>
            </template>
          </ElAutocomplete>
          <p v-if="selectedStock" class="selected-hint">
            已选择: {{ selectedStock.name }} ({{ selectedStock.ts_code }})
          </p>
        </div>
        
        <!-- 大盘分析提示 -->
        <div v-if="newTaskType === 'market'" class="market-hint">
          <TrendCharts class="hint-icon" />
          <p>将对整体市场进行综合分析，包括大盘指数、板块表现、市场情绪等。</p>
        </div>
      </div>
      
      <template #footer>
        <div class="dialog-footer">
          <button class="cancel-btn" @click="createDialogVisible = false">取消</button>
          <button 
            class="confirm-btn" 
            :disabled="isCreating || (newTaskType === 'stock' && !newStockCode.trim())"
            @click="handleCreateTask"
          >
            <Loading v-if="isCreating" class="spinning" />
            <span>{{ isCreating ? '创建中...' : '开始分析' }}</span>
          </button>
        </div>
      </template>
    </ElDialog>
    
    <!-- 筛选栏 -->
    <div class="filter-bar">
      <div class="filter-group">
        <button 
          v-for="item in [
            { value: 'all', label: '全部' },
            { value: 'running', label: '进行中' },
            { value: 'completed', label: '已完成' },
            { value: 'failed', label: '失败' },
          ]"
          :key="item.value"
          class="filter-btn"
          :class="{ active: statusFilter === item.value }"
          @click="statusFilter = item.value"
        >
          {{ item.label }}
        </button>
      </div>
      
      <div class="filter-right">
        <div class="type-selector">
          <Filter class="selector-icon" />
          <select v-model="typeFilter" class="type-select">
            <option value="all">全部类型</option>
            <option value="stock_analysis">个股分析</option>
            <option value="market_overview">大盘分析</option>
            <option value="news_sentiment">舆情分析</option>
            <option value="custom_query">自定义查询</option>
          </select>
        </div>
      </div>
    </div>
    
    <!-- 任务列表 -->
    <div class="task-list">
      <!-- 加载状态 -->
      <template v-if="isLoading && filteredTasks.length === 0">
        <div v-for="i in 3" :key="i" class="task-card skeleton">
          <div class="skeleton-line title" />
          <div class="skeleton-line content" />
          <div class="skeleton-line footer" />
        </div>
      </template>
      
      <!-- 任务卡片 -->
      <div
        v-for="(task, index) in filteredTasks"
        :key="task.task_id"
        class="task-card"
        :class="[`status-${task.status}`, { 'deleting': deletingTaskId === task.task_id }]"
        :style="{ animationDelay: `${index * 0.03}s` }"
        @click="viewTask(task.task_id)"
      >
        <!-- 左侧状态条 -->
        <div 
          class="status-bar"
          :style="{ background: getStatusConfig(task.status).borderColor }"
        />
        
        <!-- 卡片内容 -->
        <div class="card-content">
          <!-- 顶部行：类型 + 状态 + 删除按钮 -->
          <div class="card-header">
            <div class="task-type">
              <span 
                class="type-icon"
                :style="{ background: getTaskTypeConfig(task.task_type).color + '15', color: getTaskTypeConfig(task.task_type).color }"
              >
                <component :is="getTaskTypeConfig(task.task_type).icon" />
              </span>
              <span class="type-label">{{ getTaskTypeConfig(task.task_type).label }}</span>
            </div>
            
            <div class="header-right">
              <!-- 状态徽章 -->
              <div 
                class="status-badge"
                :style="{ 
                  color: getStatusConfig(task.status).color,
                  background: getStatusConfig(task.status).bgColor,
                }"
              >
                <component 
                  :is="getStatusConfig(task.status).icon" 
                  :class="{ spinning: task.status === 'running' }"
                />
                <span>{{ getStatusConfig(task.status).label }}</span>
              </div>
              
              <!-- 删除按钮 -->
              <button 
                v-if="isDeletable(task)"
                class="delete-btn"
                :class="{ 'is-loading': deletingTaskId === task.task_id }"
                :disabled="deletingTaskId === task.task_id"
                @click="handleDeleteTask($event, task)"
                title="删除任务"
              >
                <Loading v-if="deletingTaskId === task.task_id" class="spinning" />
                <Delete v-else />
              </button>
            </div>
          </div>
          
          <!-- 中间行：股票名称 / 查询内容 -->
          <div class="card-body">
            <!-- 股票名称标签 -->
            <div v-if="task.ts_codes?.length > 0" class="stock-tags">
              <span 
                v-for="code in (task.ts_codes || []).slice(0, 3)" 
                :key="code" 
                class="stock-tag"
                :title="code"
              >
                {{ getStockDisplayName(task, code) }}
              </span>
              <span v-if="(task.ts_codes?.length || 0) > 3" class="more-tag">
                +{{ task.ts_codes.length - 3 }}
              </span>
            </div>
            
            <!-- 查询内容 -->
            <p v-if="task.query" class="query-text">{{ task.query }}</p>
            
            <!-- 进度条 (运行中) -->
            <div v-if="task.status === 'running'" class="progress-section">
              <div class="progress-bar-wrapper">
                <div 
                  class="progress-bar-fill"
                  :style="{ width: (getProgress(task.task_id)?.progress || 0) + '%' }"
                />
              </div>
              <span class="progress-text">{{ getProgress(task.task_id)?.message || '处理中...' }}</span>
            </div>
            
            <!-- 结果摘要 (已完成) -->
            <div v-if="task.status === 'completed' && task.result" class="result-section">
              <!-- 信号标签 -->
              <span 
                class="signal-badge"
                :style="{ 
                  color: getSignalConfig(task.result.signal).color,
                  background: getSignalConfig(task.result.signal).bgColor,
                }"
              >
                {{ getSignalConfig(task.result.signal).label }}
              </span>
              
              <!-- 置信度进度条 -->
              <div class="confidence-wrapper">
                <span class="confidence-label">置信度:</span>
                <div class="confidence-bar">
                  <div 
                    class="confidence-fill"
                    :style="{ 
                      width: (task.result.confidence * 100) + '%',
                      background: getConfidenceColor(task.result.confidence),
                    }"
                  />
                </div>
                <span 
                  class="confidence-value"
                  :style="{ color: getConfidenceColor(task.result.confidence) }"
                >
                  {{ (task.result.confidence * 100).toFixed(0) }}%
                </span>
              </div>
            </div>
          </div>
          
          <!-- 底部行：时间 + 耗时 -->
          <div class="card-footer">
            <span class="time-info">
              <Clock class="footer-icon" />
              {{ formatTime(task.created_at) }}
            </span>
            <span v-if="task.execution_time_ms" class="duration-info">
              <Timer class="footer-icon" />
              {{ (task.execution_time_ms / 1000).toFixed(1) }}s
            </span>
            <span class="view-detail">
              查看详情
              <ArrowRight class="arrow-icon" />
            </span>
          </div>
        </div>
      </div>
      
      <!-- 空状态 -->
      <div v-if="!isLoading && filteredTasks.length === 0" class="empty-state">
        <div class="empty-icon">
          <DataAnalysis />
        </div>
        <h3 class="empty-title">暂无分析任务</h3>
        <p class="empty-desc">创建您的第一个分析任务，开始智能投资之旅</p>
        <button class="empty-btn" @click="openCreateDialog">
          <Plus />
          新建任务
        </button>
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
.analysis-list-view {
  max-width: 960px;
  margin: 0 auto;
  padding: 24px;
}

// ==================== 页面头部 ====================

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
}

.header-left {
  display: flex;
  align-items: baseline;
  gap: 12px;
}

.page-title {
  font-size: 26px;
  font-weight: 700;
  color: var(--text-primary);
  margin: 0;
}

.task-count {
  font-size: 14px;
  color: var(--text-tertiary);
}

.create-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 10px 20px;
  border-radius: 10px;
  border: none;
  background: var(--primary-500);
  color: #fff;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  
  svg {
    width: 18px;
    height: 18px;
  }
  
  &:hover {
    background: var(--primary-600);
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
  }
}

// ==================== 筛选栏 ====================

.filter-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  padding: 12px 16px;
  background: var(--bg-card);
  border-radius: 12px;
  border: 1px solid var(--border-light);
}

.filter-group {
  display: flex;
  gap: 6px;
}

.filter-btn {
  padding: 8px 16px;
  border-radius: 8px;
  border: 1px solid transparent;
  background: transparent;
  color: var(--text-secondary);
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.15s;
  
  &:hover {
    background: var(--bg-secondary);
  }
  
  &.active {
    background: var(--primary-50);
    color: var(--primary-600);
    border-color: var(--primary-200);
  }
}

.filter-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.type-selector {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--bg-secondary);
  border-radius: 8px;
  border: 1px solid var(--border-light);
  
  .selector-icon {
    width: 16px;
    height: 16px;
    color: var(--text-tertiary);
  }
}

.type-select {
  border: none;
  background: transparent;
  color: var(--text-secondary);
  font-size: 13px;
  outline: none;
  cursor: pointer;
  padding-right: 8px;
  
  option {
    padding: 8px;
  }
}

// ==================== 任务列表 ====================

.task-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

// ==================== 任务卡片 ====================

.task-card {
  display: flex;
  background: var(--bg-card);
  border-radius: 12px;
  border: 1px solid var(--border-light);
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
  cursor: pointer;
  transition: all 0.2s ease;
  overflow: hidden;
  animation: slideIn 0.3s ease-out both;
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08);
    border-color: var(--border-color);
    
    .view-detail {
      opacity: 1;
      transform: translateX(0);
    }
    
    .arrow-icon {
      transform: translateX(4px);
    }
    
    .delete-btn {
      opacity: 1;
    }
  }
  
  &.deleting {
    opacity: 0.6;
    pointer-events: none;
  }
  
  // 骨架屏
  &.skeleton {
    padding: 16px 20px;
    
    .skeleton-line {
      background: linear-gradient(90deg, var(--bg-tertiary) 25%, var(--bg-secondary) 50%, var(--bg-tertiary) 75%);
      background-size: 200% 100%;
      animation: shimmer 1.5s infinite;
      border-radius: 4px;
      
      &.title {
        width: 40%;
        height: 20px;
        margin-bottom: 12px;
      }
      
      &.content {
        width: 60%;
        height: 16px;
        margin-bottom: 8px;
      }
      
      &.footer {
        width: 30%;
        height: 14px;
      }
    }
  }
}

.status-bar {
  width: 4px;
  flex-shrink: 0;
}

.card-content {
  flex: 1;
  padding: 14px 18px;
  min-width: 0;
}

// ==================== 卡片头部 ====================

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}

.task-type {
  display: flex;
  align-items: center;
  gap: 10px;
}

.type-icon {
  width: 32px;
  height: 32px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  
  svg {
    width: 16px;
    height: 16px;
  }
}

.type-label {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
}

.header-right {
  display: flex;
  align-items: center;
  gap: 8px;
}

.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 5px 10px;
  border-radius: 20px;
  font-size: 12px;
  font-weight: 500;
  
  svg {
    width: 12px;
    height: 12px;
    
    &.spinning {
      animation: spin 1s linear infinite;
    }
  }
}

.delete-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  border-radius: 6px;
  border: none;
  background: transparent;
  color: var(--text-tertiary);
  cursor: pointer;
  opacity: 0;
  transition: all 0.2s;
  
  svg {
    width: 16px;
    height: 16px;
  }
  
  &:hover {
    background: rgba(239, 68, 68, 0.1);
    color: #ef4444;
  }
  
  &.is-loading {
    opacity: 1;
    color: var(--text-tertiary);
  }
  
  &:disabled {
    cursor: not-allowed;
  }
}

// ==================== 卡片主体 ====================

.card-body {
  margin-bottom: 10px;
}

.stock-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-bottom: 8px;
}

.stock-tag {
  display: inline-block;
  padding: 4px 10px;
  background: var(--primary-900);
  color: #fff;
  font-size: 12px;
  font-weight: 600;
  border-radius: 6px;
  max-width: 100px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.more-tag {
  padding: 4px 8px;
  background: var(--bg-tertiary);
  color: var(--text-tertiary);
  font-size: 12px;
  border-radius: 6px;
}

.query-text {
  color: var(--text-secondary);
  font-size: 13px;
  margin: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

// ==================== 进度条 ====================

.progress-section {
  margin-top: 8px;
}

.progress-bar-wrapper {
  height: 6px;
  background: var(--bg-tertiary);
  border-radius: 3px;
  overflow: hidden;
  margin-bottom: 6px;
}

.progress-bar-fill {
  height: 100%;
  background: linear-gradient(90deg, #f59e0b, #fbbf24);
  border-radius: 3px;
  transition: width 0.3s ease;
  position: relative;
  
  &::after {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
    animation: shimmer 1.5s infinite;
  }
}

.progress-text {
  font-size: 12px;
  color: var(--text-tertiary);
}

// ==================== 结果摘要 ====================

.result-section {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-top: 8px;
}

.signal-badge {
  display: inline-block;
  padding: 5px 12px;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 700;
}

.confidence-wrapper {
  display: flex;
  align-items: center;
  gap: 8px;
}

.confidence-label {
  font-size: 12px;
  color: var(--text-tertiary);
}

.confidence-bar {
  width: 80px;
  height: 6px;
  background: var(--bg-tertiary);
  border-radius: 3px;
  overflow: hidden;
}

.confidence-fill {
  height: 100%;
  border-radius: 3px;
  transition: width 0.3s ease;
}

.confidence-value {
  font-size: 13px;
  font-weight: 600;
  min-width: 36px;
}

// ==================== 卡片底部 ====================

.card-footer {
  display: flex;
  align-items: center;
  gap: 16px;
  padding-top: 10px;
  border-top: 1px solid var(--border-light);
}

.time-info,
.duration-info {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  color: var(--text-tertiary);
}

.footer-icon {
  width: 12px;
  height: 12px;
}

.view-detail {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-left: auto;
  font-size: 12px;
  color: var(--primary-500);
  font-weight: 500;
  opacity: 0;
  transform: translateX(-8px);
  transition: all 0.2s;
}

.arrow-icon {
  width: 14px;
  height: 14px;
  transition: transform 0.2s;
}

// ==================== 空状态 ====================

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 60px 24px;
  text-align: center;
}

.empty-icon {
  width: 72px;
  height: 72px;
  border-radius: 20px;
  background: var(--bg-tertiary);
  display: flex;
  align-items: center;
  justify-content: center;
  margin-bottom: 20px;
  color: var(--text-tertiary);
  
  svg {
    width: 36px;
    height: 36px;
  }
}

.empty-title {
  font-size: 18px;
  font-weight: 600;
  color: var(--text-primary);
  margin: 0 0 8px;
}

.empty-desc {
  font-size: 14px;
  color: var(--text-tertiary);
  margin: 0 0 24px;
}

.empty-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 12px 24px;
  border-radius: 10px;
  border: none;
  background: var(--primary-500);
  color: #fff;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  
  svg {
    width: 18px;
    height: 18px;
  }
  
  &:hover {
    background: var(--primary-600);
    transform: translateY(-1px);
  }
}

// ==================== 动画 ====================

@keyframes slideIn {
  from {
    opacity: 0;
    transform: translateY(8px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@keyframes shimmer {
  from {
    background-position: -200% 0;
  }
  to {
    background-position: 200% 0;
  }
}

// ==================== 新建任务弹窗 ====================

:deep(.create-task-dialog) {
  .el-dialog__header {
    padding: 20px 24px 16px;
    border-bottom: 1px solid var(--border-light);
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

.dialog-content {
  .form-item {
    margin-bottom: 20px;
    
    &:last-child {
      margin-bottom: 0;
    }
  }
  
  .form-label {
    display: block;
    font-size: 14px;
    font-weight: 500;
    color: var(--text-primary);
    margin-bottom: 10px;
  }
}

.task-type-group {
  display: flex;
  width: 100%;
  
  :deep(.el-radio-button) {
    flex: 1;
    
    .el-radio-button__inner {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 14px 20px;
      border-radius: 8px !important;
      border: 1px solid var(--border-color) !important;
      background: var(--bg-card);
      font-weight: 500;
      transition: all 0.2s;
    }
    
    &:first-child .el-radio-button__inner {
      border-radius: 8px 0 0 8px !important;
    }
    
    &:last-child .el-radio-button__inner {
      border-radius: 0 8px 8px 0 !important;
    }
    
    &.is-active .el-radio-button__inner {
      background: var(--primary-50);
      border-color: var(--primary-500) !important;
      color: var(--primary-600);
    }
  }
  
  .type-icon {
    width: 18px;
    height: 18px;
  }
}

.stock-input {
  width: 100%;
  
  :deep(.el-input__wrapper) {
    padding: 8px 12px;
    border-radius: 8px;
  }
}

.stock-suggestion {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 4px 0;
  
  .stock-name {
    font-weight: 500;
    color: var(--text-primary);
  }
  
  .stock-code {
    font-size: 12px;
    color: var(--text-tertiary);
  }
}

.selected-hint {
  margin: 8px 0 0;
  font-size: 12px;
  color: var(--primary-600);
}

.market-hint {
  display: flex;
  gap: 12px;
  padding: 16px;
  background: var(--primary-50);
  border-radius: 10px;
  border: 1px solid var(--primary-200);
  
  .hint-icon {
    width: 24px;
    height: 24px;
    color: var(--primary-500);
    flex-shrink: 0;
  }
  
  p {
    margin: 0;
    font-size: 14px;
    color: var(--primary-700);
    line-height: 1.5;
  }
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}

.cancel-btn {
  padding: 10px 20px;
  border-radius: 8px;
  border: 1px solid var(--border-color);
  background: var(--bg-card);
  color: var(--text-secondary);
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-secondary);
    border-color: var(--border-dark);
  }
}

.confirm-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 10px 24px;
  border-radius: 8px;
  border: none;
  background: var(--primary-500);
  color: #fff;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  
  svg {
    width: 16px;
    height: 16px;
    
    &.spinning {
      animation: spin 1s linear infinite;
    }
  }
  
  &:hover:not(:disabled) {
    background: var(--primary-600);
  }
  
  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
}

// ==================== 响应式 ====================

@media (max-width: 640px) {
  .analysis-list-view {
    padding: 16px;
  }
  
  .page-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 16px;
  }
  
  .filter-bar {
    flex-direction: column;
    gap: 12px;
  }
  
  .filter-group {
    width: 100%;
    overflow-x: auto;
  }
  
  .result-section {
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
  }
  
  .confidence-wrapper {
    width: 100%;
  }
  
  .confidence-bar {
    flex: 1;
  }
  
  .delete-btn {
    opacity: 1;
  }
}
</style>
