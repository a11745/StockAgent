<script setup lang="ts">
/**
 * 自选股卡片
 * 
 * 视觉升级版 - 增加悬停效果和涨跌色块
 */
import { ref, onMounted, computed } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { useMarketStore } from '@/stores/market'
import { ElSkeleton } from 'element-plus'
import { ArrowRight, Plus, ArrowUp, ArrowDown } from '@element-plus/icons-vue'
import type { StockQuote } from '@/api'

defineProps<{
  loading?: boolean
}>()

const router = useRouter()
const userStore = useUserStore()
const marketStore = useMarketStore()

// 行情数据
const quotes = ref<Map<string, StockQuote>>(new Map())
const quoteLoading = ref(true)

// 自选股列表
const watchlist = computed(() => (userStore.watchlist || []).slice(0, 5))

// 加载行情
onMounted(async () => {
  if (watchlist.value.length > 0) {
    try {
      const quoteList = await marketStore.fetchQuotes(watchlist.value)
      quoteList.forEach((q) => quotes.value.set(q.ts_code, q))
    } finally {
      quoteLoading.value = false
    }
  } else {
    quoteLoading.value = false
  }
})

// 获取行情
function getQuote(tsCode: string): StockQuote | undefined {
  return quotes.value.get(tsCode)
}

// 涨跌类型
function getChangeType(pctChg?: number): 'up' | 'down' | 'flat' {
  if (!pctChg) return 'flat'
  return pctChg > 0 ? 'up' : pctChg < 0 ? 'down' : 'flat'
}

// 格式化涨跌幅
function formatChange(pctChg?: number): string {
  if (pctChg === undefined || pctChg === null) return '--'
  const sign = pctChg > 0 ? '+' : ''
  return `${sign}${pctChg.toFixed(2)}%`
}
</script>

<template>
  <div class="watchlist-card card">
    <div class="card-header">
      <span class="header-title">自选股</span>
      <router-link to="/watchlist" class="view-all">
        全部
        <ElIcon :size="14"><ArrowRight /></ElIcon>
      </router-link>
    </div>
    
    <div class="card-body">
      <el-skeleton v-if="loading || quoteLoading" :rows="4" animated />
      
      <template v-else>
        <div v-if="watchlist.length > 0" class="stock-list">
          <div
            v-for="(code, index) in watchlist"
            :key="code"
            class="stock-item"
            :style="{ animationDelay: `${index * 0.05}s` }"
            @click="router.push(`/stock/${code}`)"
          >
            <div class="stock-info">
              <span class="stock-name">{{ getQuote(code)?.name || code }}</span>
              <span class="stock-code">{{ code }}</span>
            </div>
            
            <div class="stock-quote" :class="getChangeType(getQuote(code)?.pct_chg)">
              <span class="quote-price">{{ getQuote(code)?.price?.toFixed(2) || '--' }}</span>
              <span class="quote-change">
                <ElIcon v-if="getChangeType(getQuote(code)?.pct_chg) === 'up'" :size="12"><ArrowUp /></ElIcon>
                <ElIcon v-else-if="getChangeType(getQuote(code)?.pct_chg) === 'down'" :size="12"><ArrowDown /></ElIcon>
                {{ formatChange(getQuote(code)?.pct_chg) }}
              </span>
            </div>
          </div>
        </div>
        
        <div v-else class="empty-state">
          <div class="empty-icon">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
              <path d="M11.48 3.499a.562.562 0 011.04 0l2.125 5.111a.563.563 0 00.475.345l5.518.442c.499.04.701.663.321.988l-4.204 3.602a.563.563 0 00-.182.557l1.285 5.385a.562.562 0 01-.84.61l-4.725-2.885a.562.562 0 00-.586 0L6.982 20.54a.562.562 0 01-.84-.61l1.285-5.386a.562.562 0 00-.182-.557l-4.204-3.602a.562.562 0 01.321-.988l5.518-.442a.563.563 0 00.475-.345L11.48 3.5z"/>
            </svg>
          </div>
          <p class="empty-text">暂无自选股</p>
          <p class="empty-hint">添加股票到自选，随时关注行情</p>
          <button class="add-btn" @click="router.push('/stock')">
            <Plus />
            添加自选
          </button>
        </div>
      </template>
    </div>
  </div>
</template>

<style lang="scss" scoped>
.watchlist-card {
  .card-header {
    .header-title {
      font-weight: 600;
      color: var(--text-primary);
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
    padding: 8px 0 12px;
  }
}

.stock-list {
  display: flex;
  flex-direction: column;
}

.stock-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 14px 24px;
  cursor: pointer;
  transition: all var(--transition-fast);
  animation: fadeSlide 0.3s ease-out both;
  
  &:hover {
    background: var(--bg-hover);
  }
  
  .stock-info {
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  
  .stock-name {
    font-size: 14px;
    font-weight: 600;
    color: var(--text-primary);
  }
  
  .stock-code {
    font-size: 12px;
    color: var(--text-tertiary);
    font-family: 'SF Mono', Consolas, monospace;
  }
  
  .stock-quote {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 3px;
    
    &.up {
      .quote-price,
      .quote-change {
        color: var(--stock-up);
      }
    }
    
    &.down {
      .quote-price,
      .quote-change {
        color: var(--stock-down);
      }
    }
    
    &.flat {
      .quote-price,
      .quote-change {
        color: var(--stock-flat);
      }
    }
  }
  
  .quote-price {
    font-size: 15px;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
  }
  
  .quote-change {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    font-size: 12px;
    font-weight: 500;
  }
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 32px 24px;
  text-align: center;
  
  .empty-icon {
    width: 56px;
    height: 56px;
    border-radius: 14px;
    background: var(--bg-tertiary);
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 16px;
    
    svg {
      width: 28px;
      height: 28px;
      color: var(--text-tertiary);
    }
  }
  
  .empty-text {
    font-size: 15px;
    font-weight: 600;
    color: var(--text-secondary);
    margin: 0 0 6px;
  }
  
  .empty-hint {
    font-size: 13px;
    color: var(--text-tertiary);
    margin: 0 0 20px;
  }
  
  .add-btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 10px 20px;
    border-radius: 10px;
    border: none;
    background: var(--primary-500);
    color: #fff;
    font-size: 14px;
    font-weight: 500;
    cursor: pointer;
    transition: all var(--transition-fast);
    
    svg {
      width: 16px;
      height: 16px;
    }
    
    &:hover {
      background: var(--primary-600);
      transform: translateY(-2px);
      box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
    }
  }
}

@keyframes fadeSlide {
  from {
    opacity: 0;
    transform: translateX(-8px);
  }
  to {
    opacity: 1;
    transform: translateX(0);
  }
}
</style>
