<script setup lang="ts">
/**
 * 分析详情页 - 展示 Agent 分析结果
 * 
 * 支持 Markdown 格式渲染
 */

import { ref, computed, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { 
  ElCard, 
  ElDescriptions, 
  ElDescriptionsItem, 
  ElTag, 
  ElButton,
  ElDivider,
  ElEmpty,
  ElSkeleton,
} from 'element-plus'
import { Refresh, Back } from '@element-plus/icons-vue'
import AgentThinking from '@/components/agent/AgentThinking.vue'
import TaskStatusBadge from '@/components/agent/TaskStatusBadge.vue'
import { useTask } from '@/hooks'
import { useWebSocket } from '@/hooks'
import { taskApi } from '@/api'
import { TaskStatus, SignalType } from '@/api/types'
import type { Task, AnalysisScore } from '@/api/types'
import { marked } from 'marked'

// 配置 marked
marked.setOptions({
  breaks: true,  // 支持换行符转换为 <br>
  gfm: true,     // 支持 GitHub Flavored Markdown
})

const route = useRoute()
const { selectTask, currentTask, currentThoughts, getTaskStatusLabel } = useTask()
const { subscribeTask, isConnected } = useWebSocket()

// ==================== 状态 ====================

const loading = ref(false)
const task = ref<Task | null>(null)

const taskId = computed(() => route.params.id as string)

// ==================== 信号配置 ====================

interface SignalConfig {
  label: string
  type: 'danger' | 'warning' | 'info' | 'success'
  color: string
}

const signalConfigs: Record<SignalType, SignalConfig> = {
  [SignalType.STRONG_BUY]: { label: '强烈买入', type: 'success', color: '#67c23a' },
  [SignalType.BUY]: { label: '买入', type: 'success', color: '#95d475' },
  [SignalType.HOLD]: { label: '持有', type: 'info', color: '#909399' },
  [SignalType.SELL]: { label: '卖出', type: 'warning', color: '#e6a23c' },
  [SignalType.STRONG_SELL]: { label: '强烈卖出', type: 'danger', color: '#f56c6c' },
}

// ==================== 计算属性 ====================

const result = computed(() => task.value?.result)
const signal = computed(() => result.value?.signal)
const signalConfig = computed(() => signal.value ? signalConfigs[signal.value] : null)

const thoughts = computed(() => 
  currentTask.value?.task_id === taskId.value ? currentThoughts.value : []
)

const isRunning = computed(() => 
  task.value?.status === TaskStatus.RUNNING || task.value?.status === TaskStatus.PENDING
)

// ==================== 方法 ====================

async function fetchTask(): Promise<void> {
  loading.value = true
  try {
    task.value = await taskApi.getTask(taskId.value)
    selectTask(taskId.value)
    
    // 订阅实时更新
    if (isConnected.value && isRunning.value) {
      subscribeTask(taskId.value)
    }
  } finally {
    loading.value = false
  }
}

function getScoreColor(score: number): string {
  if (score >= 80) return '#67c23a'
  if (score >= 60) return '#e6a23c'
  return '#f56c6c'
}

function formatScore(scores: AnalysisScore | undefined): { label: string; value: number }[] {
  if (!scores) return []
  
  const labels: Record<keyof AnalysisScore, string> = {
    fundamental: '基本面',
    technical: '技术面',
    sentiment: '舆情',
    valuation: '估值',
  }
  
  return Object.entries(scores)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => ({
      label: labels[k as keyof AnalysisScore],
      value: v as number,
    }))
}

/**
 * 格式化分析文本
 * - 将中文方括号【】转换为 Markdown 标题
 * - 处理其他特殊格式
 */
function formatAnalysisText(text: string | undefined): string {
  if (!text) return ''
  
  // 预处理：将【标题】转换为 Markdown 标题格式
  let formatted = text
    // 【xxx】 转换为 ### xxx
    .replace(/【([^】]+)】/g, '\n\n### $1\n\n')
    // 处理数字列表格式：1. 2. 3. 确保前面有换行
    .replace(/(\d+)\.\s*\*\*/g, '\n$1. **')
    // 处理孤立的 ** 标记（确保粗体正确渲染）
    .replace(/\*\*([^*]+)\*\*/g, '**$1**')
    // 处理长段落，在句号后添加适当的换行（如果后面紧跟内容）
    .replace(/。(?=[^\n\s])/g, '。\n\n')
    // 清理多余的空行
    .replace(/\n{4,}/g, '\n\n\n')
  
  // 使用 marked 渲染 Markdown
  return marked.parse(formatted) as string
}

/**
 * 获取股票显示名称
 */
function getStockDisplayName(code: string): string {
  const stockInfo = task.value?.stock_names?.find(s => s.ts_code === code)
  return stockInfo ? `${stockInfo.name} (${code})` : code
}

// ==================== 生命周期 ====================

onMounted(() => {
  fetchTask()
})

watch(() => route.params.id, () => {
  fetchTask()
})
</script>

<template>
  <div class="analysis-detail">
    <!-- 头部 -->
    <div class="page-header">
      <ElButton :icon="Back" @click="$router.back()">返回</ElButton>
      <h1>分析报告</h1>
      <ElButton :icon="Refresh" :loading="loading" @click="fetchTask">刷新</ElButton>
    </div>
    
    <ElSkeleton v-if="loading && !task" :rows="10" animated />
    
    <template v-else-if="task">
      <!-- 基本信息 -->
      <ElCard class="info-card">
        <template #header>
          <div class="card-header">
            <span>任务信息</span>
            <TaskStatusBadge 
              :status="task.status" 
              :progress="currentTask?.task_id === taskId ? (currentThoughts.length * 20) : 0"
              show-progress
            />
          </div>
        </template>
        
        <ElDescriptions :column="3" border>
          <ElDescriptionsItem label="任务 ID">
            <code>{{ task.task_id }}</code>
          </ElDescriptionsItem>
          <ElDescriptionsItem label="任务类型">
            {{ task.task_type }}
          </ElDescriptionsItem>
          <ElDescriptionsItem label="状态">
            {{ getTaskStatusLabel(task.status) }}
          </ElDescriptionsItem>
          <ElDescriptionsItem label="分析标的">
            <ElTag 
              v-for="code in task.ts_codes" 
              :key="code" 
              class="stock-tag"
              type="primary"
            >
              {{ getStockDisplayName(code) }}
            </ElTag>
            <span v-if="task.ts_codes.length === 0">大盘分析</span>
          </ElDescriptionsItem>
          <ElDescriptionsItem label="创建时间">
            {{ new Date(task.created_at).toLocaleString() }}
          </ElDescriptionsItem>
          <ElDescriptionsItem label="执行耗时">
            {{ task.execution_time_ms ? `${(task.execution_time_ms / 1000).toFixed(1)}s` : '-' }}
          </ElDescriptionsItem>
        </ElDescriptions>
      </ElCard>
      
      <!-- Agent 推理过程 -->
      <ElCard v-if="isRunning || thoughts.length > 0" class="thinking-card">
        <template #header>Agent 推理过程</template>
        <AgentThinking 
          :thoughts="thoughts" 
          :status="task.status"
          max-height="350px"
        />
      </ElCard>
      
      <!-- 分析结果 -->
      <ElCard v-if="result" class="result-card">
        <template #header>
          <div class="card-header">
            <span>分析结论</span>
            <div class="header-right">
              <span v-if="result.confidence" class="confidence-badge">
                置信度: {{ (result.confidence * 100).toFixed(0) }}%
              </span>
              <ElTag v-if="signalConfig" :type="signalConfig.type" size="large" effect="dark">
                {{ signalConfig.label }}
              </ElTag>
            </div>
          </div>
        </template>
        
        <!-- 评分 -->
        <div class="scores-section">
          <div 
            v-for="score in formatScore(result.scores)" 
            :key="score.label" 
            class="score-item"
          >
            <span class="score-label">{{ score.label }}</span>
            <div class="score-bar">
              <div 
                class="score-fill" 
                :style="{ width: `${score.value}%`, backgroundColor: getScoreColor(score.value) }"
              />
            </div>
            <span class="score-value">{{ score.value }}</span>
          </div>
        </div>
        
        <ElDivider />
        
        <!-- 摘要 -->
        <div class="summary-section">
          <h3>📋 综合摘要</h3>
          <div class="markdown-content" v-html="formatAnalysisText(result.summary)" />
        </div>
        
        <!-- 详细分析 -->
        <div v-if="result.fundamental_analysis" class="analysis-section">
          <h3>📈 基本面分析</h3>
          <div class="markdown-content" v-html="formatAnalysisText(result.fundamental_analysis)" />
        </div>
        
        <div v-if="result.technical_analysis" class="analysis-section">
          <h3>📉 技术分析</h3>
          <div class="markdown-content" v-html="formatAnalysisText(result.technical_analysis)" />
        </div>
        
        <div v-if="result.sentiment_analysis" class="analysis-section">
          <h3>💬 舆情分析</h3>
          <div class="markdown-content" v-html="formatAnalysisText(result.sentiment_analysis)" />
        </div>
        
        <!-- 风险提示 -->
        <div v-if="result.risks && result.risks.length > 0" class="risks-section">
          <h3>⚠️ 风险提示</h3>
          <ul>
            <li v-for="(risk, index) in result.risks" :key="index">{{ risk }}</li>
          </ul>
        </div>
        
        <!-- 目标价 -->
        <div v-if="result.target_price || result.stop_loss_price" class="price-targets">
          <div v-if="result.target_price" class="price-item target">
            <span class="label">目标价</span>
            <span class="value">¥{{ result.target_price.toFixed(2) }}</span>
          </div>
          <div v-if="result.stop_loss_price" class="price-item stoploss">
            <span class="label">止损价</span>
            <span class="value">¥{{ result.stop_loss_price.toFixed(2) }}</span>
          </div>
        </div>
      </ElCard>
      
      <!-- 失败信息 -->
      <ElCard v-if="task.status === TaskStatus.FAILED" class="error-card">
        <template #header>错误信息</template>
        <p class="error-message">{{ task.error_message || '未知错误' }}</p>
      </ElCard>
      
      <!-- 空状态 -->
      <ElEmpty v-if="!result && !isRunning && task.status !== TaskStatus.FAILED" description="暂无分析结果" />
    </template>
    
    <ElEmpty v-else description="任务不存在" />
  </div>
</template>

<style scoped lang="scss">
.analysis-detail {
  padding: 20px;
  max-width: 1200px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 24px;
  
  h1 {
    flex: 1;
    margin: 0;
    font-size: 24px;
  }
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  
  .header-right {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  
  .confidence-badge {
    font-size: 14px;
    color: var(--el-text-color-secondary);
    background: var(--el-fill-color-light);
    padding: 4px 12px;
    border-radius: 12px;
  }
}

.info-card,
.thinking-card,
.result-card,
.error-card {
  margin-bottom: 20px;
}

.stock-tag {
  margin-right: 8px;
}

.scores-section {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 16px;
  
  .score-item {
    display: flex;
    align-items: center;
    gap: 12px;
    
    .score-label {
      width: 60px;
      font-weight: 500;
    }
    
    .score-bar {
      flex: 1;
      height: 8px;
      background: var(--el-fill-color-light);
      border-radius: 4px;
      overflow: hidden;
      
      .score-fill {
        height: 100%;
        border-radius: 4px;
        transition: width 0.5s ease;
      }
    }
    
    .score-value {
      width: 30px;
      text-align: right;
      font-weight: 600;
    }
  }
}

.summary-section,
.analysis-section,
.risks-section {
  margin-top: 24px;
  padding: 20px;
  background: var(--el-fill-color-lighter);
  border-radius: 12px;
  border-left: 4px solid var(--el-color-primary);
  
  h3 {
    margin: 0 0 16px 0;
    font-size: 17px;
    font-weight: 600;
    color: var(--el-text-color-primary);
  }
  
  ul {
    margin: 0;
    padding-left: 20px;
    
    li {
      line-height: 1.8;
      color: var(--el-text-color-regular);
      margin-bottom: 8px;
    }
  }
}

// Markdown 内容样式
.markdown-content {
  line-height: 1.8;
  color: var(--el-text-color-regular);
  
  :deep(h3) {
    font-size: 15px;
    font-weight: 600;
    color: var(--el-color-primary);
    margin: 20px 0 12px 0;
    padding-bottom: 8px;
    border-bottom: 1px dashed var(--el-border-color-lighter);
    
    &:first-child {
      margin-top: 0;
    }
  }
  
  :deep(p) {
    margin: 0 0 12px 0;
    text-align: justify;
  }
  
  :deep(strong) {
    color: var(--el-text-color-primary);
    font-weight: 600;
  }
  
  :deep(ol),
  :deep(ul) {
    margin: 12px 0;
    padding-left: 24px;
    
    li {
      margin-bottom: 8px;
      
      &::marker {
        color: var(--el-color-primary);
        font-weight: 600;
      }
    }
  }
  
  :deep(ol) {
    list-style-type: decimal;
  }
  
  :deep(ul) {
    list-style-type: disc;
  }
  
  :deep(blockquote) {
    margin: 12px 0;
    padding: 12px 16px;
    background: var(--el-fill-color);
    border-left: 3px solid var(--el-color-primary);
    border-radius: 0 8px 8px 0;
    
    p {
      margin: 0;
    }
  }
  
  :deep(code) {
    background: var(--el-fill-color);
    padding: 2px 6px;
    border-radius: 4px;
    font-family: 'SF Mono', Consolas, monospace;
    font-size: 0.9em;
  }
  
  :deep(hr) {
    border: none;
    border-top: 1px dashed var(--el-border-color);
    margin: 20px 0;
  }
}

.price-targets {
  display: flex;
  gap: 24px;
  margin-top: 24px;
  padding: 16px;
  background: var(--el-fill-color-lighter);
  border-radius: 8px;
  
  .price-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    
    .label {
      font-size: 12px;
      color: var(--el-text-color-secondary);
    }
    
    .value {
      font-size: 24px;
      font-weight: 600;
    }
    
    &.target .value {
      color: var(--el-color-success);
    }
    
    &.stoploss .value {
      color: var(--el-color-danger);
    }
  }
}

.error-card {
  .error-message {
    color: var(--el-color-danger);
    margin: 0;
  }
}
</style>
