<script setup lang="ts">
/**
 * 任务进度卡片
 * 
 * 视觉升级版 - 带进度条和状态徽章
 */
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { useTaskStore } from '@/stores/task'
import { TaskStatus } from '@/api/types'
import { Timer, ArrowRight, TrendCharts, DataAnalysis, Document } from '@element-plus/icons-vue'

const router = useRouter()
const taskStore = useTaskStore()

// 运行中的任务
const runningTasks = computed(() => {
  const tasks = taskStore.tasks || []
  return tasks
    .filter(t => t.status === TaskStatus.PENDING || t.status === TaskStatus.RUNNING)
    .slice(0, 5)
})

// 任务类型配置
const taskTypeConfig: Record<string, { label: string; icon: typeof TrendCharts; color: string }> = {
  stock_analysis: { label: '个股分析', icon: DataAnalysis, color: '#3b82f6' },
  market_overview: { label: '大盘分析', icon: TrendCharts, color: '#8b5cf6' },
  strategy_backtest: { label: '策略回测', icon: Timer, color: '#06b6d4' },
  news_sentiment: { label: '舆情分析', icon: Document, color: '#f59e0b' },
  report_generate: { label: '研报生成', icon: Document, color: '#10b981' },
  custom_query: { label: '自定义查询', icon: Document, color: '#6366f1' },
}

function getTaskConfig(taskType: string) {
  return taskTypeConfig[taskType] || { label: taskType, icon: Document, color: '#64748b' }
}

function getProgress(taskId: string) {
  return taskStore.getProgress(taskId)?.progress || 0
}

function getMessage(taskId: string) {
  return taskStore.getProgress(taskId)?.message || '处理中...'
}

function navigateToTask(taskId: string) {
  router.push(`/analysis/${taskId}`)
}
</script>

<template>
  <div v-if="runningTasks.length > 0" class="task-progress-card card">
    <div class="card-header">
      <div class="header-left">
        <span class="header-title">进行中的任务</span>
        <span class="task-count">{{ runningTasks.length }}</span>
      </div>
      <router-link to="/analysis" class="view-all">
        查看全部
        <ElIcon :size="14"><ArrowRight /></ElIcon>
      </router-link>
    </div>
    
    <div class="card-body">
      <div class="task-list">
        <div
          v-for="(task, index) in runningTasks"
          :key="task.task_id"
          class="task-item"
          :style="{ animationDelay: `${index * 0.05}s` }"
          @click="navigateToTask(task.task_id)"
        >
          <!-- 左侧图标 -->
          <div 
            class="task-icon"
            :style="{ background: getTaskConfig(task.task_type).color + '15', color: getTaskConfig(task.task_type).color }"
          >
            <component :is="getTaskConfig(task.task_type).icon" />
          </div>
          
          <!-- 任务信息 -->
          <div class="task-content">
            <div class="task-header">
              <span class="task-type">{{ getTaskConfig(task.task_type).label }}</span>
              <div class="task-badges">
                <span 
                  v-if="task.ts_codes?.length > 0" 
                  class="stock-badge"
                >
                  {{ task.ts_codes[0] }}
                  <template v-if="task.ts_codes.length > 1">
                    +{{ task.ts_codes.length - 1 }}
                  </template>
                </span>
                <span class="status-badge running">
                  <span class="pulse-dot" />
                  运行中
                </span>
              </div>
            </div>
            
            <!-- 进度条 -->
            <div class="task-progress">
              <div class="progress-bar">
                <div 
                  class="progress-fill"
                  :style="{ 
                    width: getProgress(task.task_id) + '%',
                    background: getTaskConfig(task.task_type).color
                  }"
                >
                  <div class="progress-shine" />
                </div>
              </div>
              <span class="progress-text">{{ getMessage(task.task_id) }}</span>
            </div>
          </div>
          
          <!-- 右侧箭头 -->
          <div class="task-arrow">
            <ElIcon :size="16"><ArrowRight /></ElIcon>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
.task-progress-card {
  .card-header {
    .header-left {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    
    .header-title {
      font-weight: 600;
      color: var(--text-primary);
    }
    
    .task-count {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 22px;
      height: 22px;
      padding: 0 6px;
      font-size: 12px;
      font-weight: 600;
      color: #fff;
      background: linear-gradient(135deg, #f59e0b, #ef4444);
      border-radius: 11px;
    }
    
    .view-all {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      font-size: 13px;
      color: var(--primary-500);
      font-weight: 500;
      transition: all var(--transition-fast);
      
      &:hover {
        color: var(--primary-600);
        gap: 8px;
      }
    }
  }
  
  .card-body {
    padding: 12px 24px 20px;
  }
}

.task-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.task-item {
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 14px 16px;
  background: var(--bg-secondary);
  border-radius: var(--radius-md);
  border: 1px solid var(--border-light);
  cursor: pointer;
  transition: all var(--transition-normal);
  animation: slideIn 0.3s ease-out both;
  
  &:hover {
    background: var(--bg-hover);
    border-color: var(--border-color);
    transform: translateX(4px);
    
    .task-arrow {
      opacity: 1;
      transform: translateX(0);
    }
  }
  
  .task-icon {
    width: 40px;
    height: 40px;
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    
    svg {
      width: 20px;
      height: 20px;
    }
  }
  
  .task-content {
    flex: 1;
    min-width: 0;
  }
  
  .task-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 10px;
  }
  
  .task-type {
    font-size: 14px;
    font-weight: 600;
    color: var(--text-primary);
  }
  
  .task-badges {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  
  .stock-badge {
    font-size: 11px;
    font-weight: 500;
    color: var(--text-secondary);
    padding: 3px 8px;
    background: var(--bg-tertiary);
    border-radius: 6px;
  }
  
  .status-badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 11px;
    font-weight: 500;
    padding: 3px 8px 3px 6px;
    border-radius: 6px;
    
    &.running {
      color: var(--warning);
      background: rgba(245, 158, 11, 0.1);
    }
    
    .pulse-dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: currentColor;
      animation: pulse 1.5s ease-in-out infinite;
    }
  }
  
  .task-progress {
    .progress-bar {
      height: 6px;
      background: var(--bg-tertiary);
      border-radius: 3px;
      overflow: hidden;
      margin-bottom: 6px;
    }
    
    .progress-fill {
      height: 100%;
      border-radius: 3px;
      position: relative;
      transition: width 0.5s ease-out;
      overflow: hidden;
    }
    
    .progress-shine {
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: linear-gradient(
        90deg,
        transparent,
        rgba(255, 255, 255, 0.4),
        transparent
      );
      animation: shimmer 1.5s infinite;
    }
    
    .progress-text {
      font-size: 12px;
      color: var(--text-tertiary);
    }
  }
  
  .task-arrow {
    color: var(--text-tertiary);
    opacity: 0;
    transform: translateX(-8px);
    transition: all var(--transition-fast);
  }
}

@keyframes slideIn {
  from {
    opacity: 0;
    transform: translateX(-12px);
  }
  to {
    opacity: 1;
    transform: translateX(0);
  }
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
    transform: scale(1);
  }
  50% {
    opacity: 0.5;
    transform: scale(0.8);
  }
}

@keyframes shimmer {
  from {
    transform: translateX(-100%);
  }
  to {
    transform: translateX(100%);
  }
}

@media (max-width: 640px) {
  .task-item {
    flex-wrap: wrap;
    
    .task-header {
      flex-direction: column;
      align-items: flex-start;
      gap: 6px;
    }
  }
}
</style>
