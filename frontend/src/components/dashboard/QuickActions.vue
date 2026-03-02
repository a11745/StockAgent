<script setup lang="ts">
/**
 * 快捷操作卡片
 * 
 * 视觉升级版 - 磁贴式大按钮 + 悬停动画
 */
import { useRouter } from 'vue-router'
import { useTask } from '@/hooks'
import { ElMessage } from 'element-plus'
import { TrendCharts, Aim, Star, Document } from '@element-plus/icons-vue'

const router = useRouter()
const { analyzeMarket, isCreating } = useTask()

// 快捷操作配置
const actions = [
  {
    id: 'market',
    label: '大盘分析',
    description: 'AI 智能解读',
    icon: TrendCharts,
    gradient: 'linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%)',
    shadowColor: 'rgba(59, 130, 246, 0.35)',
    action: async () => {
      const taskId = await analyzeMarket()
      if (taskId) {
        ElMessage.success('大盘分析任务已创建')
        router.push(`/analysis/${taskId}`)
      }
    },
  },
  {
    id: 'strategy',
    label: '策略监听',
    description: '实时预警',
    icon: Aim,
    gradient: 'linear-gradient(135deg, #22c55e 0%, #16a34a 100%)',
    shadowColor: 'rgba(34, 197, 94, 0.35)',
    action: () => router.push('/strategy'),
  },
  {
    id: 'watchlist',
    label: '自选股',
    description: '个股追踪',
    icon: Star,
    gradient: 'linear-gradient(135deg, #f59e0b 0%, #d97706 100%)',
    shadowColor: 'rgba(245, 158, 11, 0.35)',
    action: () => router.push('/watchlist'),
  },
  {
    id: 'history',
    label: '分析记录',
    description: '历史报告',
    icon: Document,
    gradient: 'linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%)',
    shadowColor: 'rgba(139, 92, 246, 0.35)',
    action: () => router.push('/analysis'),
  },
]
</script>

<template>
  <div class="quick-actions card">
    <div class="card-header">
      <span class="header-title">快捷操作</span>
    </div>
    
    <div class="card-body">
      <div class="actions-grid">
        <button
          v-for="action in actions"
          :key="action.id"
          class="action-tile"
          :disabled="isCreating"
          @click="action.action"
        >
          <!-- 图标容器 -->
          <div 
            class="tile-icon"
            :style="{ 
              background: action.gradient,
              boxShadow: `0 8px 24px ${action.shadowColor}`
            }"
          >
            <component :is="action.icon" />
          </div>
          
          <!-- 文字信息 -->
          <div class="tile-content">
            <span class="tile-label">{{ action.label }}</span>
            <span class="tile-desc">{{ action.description }}</span>
          </div>
          
          <!-- 悬停光效 -->
          <div class="tile-glow" :style="{ background: action.gradient }" />
        </button>
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
.quick-actions {
  .card-header {
    .header-title {
      font-weight: 600;
      color: var(--text-primary);
    }
  }
  
  .card-body {
    padding: 20px 24px 24px;
  }
}

.actions-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 14px;
}

.action-tile {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 14px;
  padding: 24px 16px;
  border-radius: var(--radius-lg);
  border: 1px solid var(--border-light);
  background: var(--bg-secondary);
  cursor: pointer;
  transition: all var(--transition-normal);
  overflow: hidden;
  
  &::before {
    content: '';
    position: absolute;
    inset: 0;
    background: var(--gradient-card);
    opacity: 0;
    transition: opacity var(--transition-normal);
  }
  
  &:hover {
    transform: translateY(-4px);
    border-color: var(--border-color);
    box-shadow: var(--shadow-lg);
    
    &::before {
      opacity: 1;
    }
    
    .tile-icon {
      transform: scale(1.1) rotate(-3deg);
    }
    
    .tile-glow {
      opacity: 0.08;
      transform: scale(2);
    }
    
    .tile-label {
      color: var(--text-primary);
    }
  }
  
  &:active {
    transform: translateY(-2px);
  }
  
  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
    
    &:hover {
      transform: none;
      
      .tile-icon {
        transform: none;
      }
    }
  }
  
  .tile-icon {
    position: relative;
    z-index: 2;
    width: 56px;
    height: 56px;
    border-radius: 16px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #fff;
    transition: transform var(--transition-normal);
    
    svg {
      width: 26px;
      height: 26px;
    }
  }
  
  .tile-content {
    position: relative;
    z-index: 2;
    text-align: center;
  }
  
  .tile-label {
    display: block;
    font-size: 14px;
    font-weight: 600;
    color: var(--text-secondary);
    margin-bottom: 4px;
    transition: color var(--transition-fast);
  }
  
  .tile-desc {
    display: block;
    font-size: 12px;
    color: var(--text-tertiary);
  }
  
  .tile-glow {
    position: absolute;
    width: 80px;
    height: 80px;
    border-radius: 50%;
    opacity: 0;
    transition: all var(--transition-slow);
    pointer-events: none;
    filter: blur(20px);
    z-index: 1;
  }
}

@media (max-width: 480px) {
  .actions-grid {
    grid-template-columns: 1fr;
  }
  
  .action-tile {
    flex-direction: row;
    padding: 16px 20px;
    gap: 16px;
    
    .tile-icon {
      width: 48px;
      height: 48px;
      border-radius: 12px;
      
      svg {
        width: 22px;
        height: 22px;
      }
    }
    
    .tile-content {
      text-align: left;
    }
  }
}
</style>
