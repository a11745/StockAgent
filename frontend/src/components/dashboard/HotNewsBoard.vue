<script setup lang="ts">
/**
 * 热点新闻看板组件
 * 
 * 参考 NewsNow 设计风格:
 * - 多来源标签页切换
 * - 卡片式新闻列表
 * - 热度指数显示
 * - 手动刷新功能
 */

import { ref, computed, onMounted, watch } from 'vue'
import { ElTabs, ElTabPane, ElSkeleton, ElEmpty, ElButton, ElMessage, ElTooltip } from 'element-plus'
import { Refresh, Link, Sunny } from '@element-plus/icons-vue'
import { marketApi } from '@/api'
import type { HotNewsItem, HotNewsSource } from '@/api/modules/market'

// ==================== Props ====================

interface Props {
  /** 高度限制 */
  maxHeight?: string
  /** 是否显示刷新按钮 */
  showRefresh?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  maxHeight: '600px',
  showRefresh: true,
})

// ==================== 状态 ====================

const loading = ref(true)
const refreshing = ref(false)
const activeSource = ref('baidu')
const sources = ref<HotNewsSource[]>([])
const newsMap = ref<Record<string, HotNewsItem[]>>({})
const lastUpdate = ref<string>('')

// ==================== 计算属性 ====================

const currentNews = computed(() => {
  return newsMap.value[activeSource.value] || []
})

const formatHot = (hot: number) => {
  if (hot >= 10000000) {
    return `${(hot / 10000000).toFixed(1)}kw`
  }
  if (hot >= 10000) {
    return `${(hot / 10000).toFixed(1)}w`
  }
  if (hot >= 1000) {
    return `${(hot / 1000).toFixed(1)}k`
  }
  return hot.toString()
}

const formatTime = (time: string) => {
  if (!time) return ''
  const date = new Date(time)
  const now = new Date()
  const diff = now.getTime() - date.getTime()
  
  if (diff < 3600000) {
    return `${Math.floor(diff / 60000)}分钟前`
  }
  if (diff < 86400000) {
    return `${Math.floor(diff / 3600000)}小时前`
  }
  return `${date.getMonth() + 1}/${date.getDate()}`
}

// ==================== 方法 ====================

async function loadSources() {
  try {
    const res = await marketApi.getHotNewsSources()
    sources.value = res.sources
    
    // 默认选中第一个
    if (sources.value.length > 0 && !activeSource.value) {
      activeSource.value = sources.value[0].id
    }
  } catch (error) {
    console.error('Failed to load sources:', error)
  }
}

async function loadNews(source?: string) {
  const targetSource = source || activeSource.value
  
  try {
    loading.value = true
    const res = await marketApi.getHotNews(targetSource, 50)
    newsMap.value[targetSource] = res.news
    
    // 更新最后刷新时间 (优先使用来源级别的 updated_at)
    if (res.updated_at) {
      lastUpdate.value = res.updated_at
    } else if (res.news.length > 0 && res.news[0].updated_at) {
      lastUpdate.value = res.news[0].updated_at
    }
  } catch (error) {
    console.error('Failed to load news:', error)
  } finally {
    loading.value = false
  }
}

async function handleRefresh() {
  refreshing.value = true
  
  try {
    // 触发后端刷新
    await marketApi.refreshHotNews(activeSource.value)
    
    // 重新加载数据
    await loadNews()
    
    ElMessage.success(`${getSourceName(activeSource.value)} 刷新成功`)
  } catch (error) {
    console.error('Refresh failed:', error)
    ElMessage.error('刷新失败')
  } finally {
    refreshing.value = false
  }
}

function getSourceName(sourceId: string) {
  const source = sources.value.find(s => s.id === sourceId)
  return source?.name || sourceId
}

function openLink(url: string) {
  if (url) {
    window.open(url, '_blank')
  }
}

// ==================== 生命周期 ====================

onMounted(async () => {
  await loadSources()
  await loadNews()
})

watch(activeSource, (newSource) => {
  if (!newsMap.value[newSource]) {
    loadNews(newSource)
  }
})
</script>

<template>
  <div class="hot-news-board">
    <!-- 头部 -->
    <div class="board-header">
      <div class="header-title">
        <Sunny class="title-icon" />
        <span>热点资讯</span>
      </div>
      
      <div class="header-actions">
        <span v-if="lastUpdate" class="update-time">
          {{ formatTime(lastUpdate) }}更新
        </span>
        <ElTooltip v-if="showRefresh" content="刷新数据" placement="top">
          <ElButton
            :icon="Refresh"
            :loading="refreshing"
            size="small"
            circle
            @click="handleRefresh"
          />
        </ElTooltip>
      </div>
    </div>
    
    <!-- 来源标签 -->
    <ElTabs v-model="activeSource" class="source-tabs">
      <ElTabPane
        v-for="source in sources"
        :key="source.id"
        :label="source.name"
        :name="source.id"
      />
    </ElTabs>
    
    <!-- 新闻列表 -->
    <div class="news-container" :style="{ maxHeight: props.maxHeight }">
      <ElSkeleton v-if="loading" :rows="8" animated />
      
      <template v-else>
        <ElEmpty v-if="currentNews.length === 0" description="暂无热点" :image-size="60" />
        
        <div v-else class="news-list">
          <div
            v-for="(news, index) in currentNews"
            :key="`${news.source}-${index}`"
            class="news-item"
            @click="openLink(news.url)"
          >
            <span class="news-rank" :class="{ top: index < 3 }">
              {{ index + 1 }}
            </span>
            
            <div class="news-content">
              <span class="news-title">{{ news.title }}</span>
              <div class="news-meta">
                <span v-if="news.hot > 0" class="news-hot">
                  <Sunny class="hot-icon" />
                  {{ formatHot(news.hot) }}
                </span>
                <span v-if="news.extra?.label" class="news-label">
                  {{ news.extra.label }}
                </span>
                <span v-if="news.extra?.is_hot" class="news-tag hot">热</span>
                <span v-if="news.extra?.is_new" class="news-tag new">新</span>
              </div>
            </div>
            
            <Link class="link-icon" />
          </div>
        </div>
      </template>
    </div>
  </div>
</template>

<style scoped lang="scss">
.hot-news-board {
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: 16px;
  overflow: hidden;
}

// ==================== Header ====================

.board-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  border-bottom: 1px solid var(--border-light);
}

.header-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 16px;
  font-weight: 600;
  color: var(--text-primary);
  
  .title-icon {
    width: 20px;
    height: 20px;
    color: #ff6b6b;
  }
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 12px;
  
  .update-time {
    font-size: 12px;
    color: var(--text-tertiary);
  }
}

// ==================== Tabs ====================

.source-tabs {
  padding: 0 16px;
  
  :deep(.el-tabs__header) {
    margin: 0;
    border-bottom: 1px solid var(--border-light);
  }
  
  :deep(.el-tabs__nav-wrap::after) {
    display: none;
  }
  
  :deep(.el-tabs__item) {
    padding: 0 16px;
    height: 40px;
    font-size: 13px;
    color: var(--text-secondary);
    
    &:hover {
      color: var(--primary-500);
    }
    
    &.is-active {
      color: var(--primary-600);
      font-weight: 600;
    }
  }
  
  :deep(.el-tabs__active-bar) {
    background: var(--primary-500);
    height: 3px;
    border-radius: 2px;
  }
}

// ==================== News List ====================

.news-container {
  overflow-y: auto;
  padding: 8px 0;
}

.news-list {
  display: flex;
  flex-direction: column;
}

.news-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 20px;
  cursor: pointer;
  transition: all var(--transition-fast);
  
  &:hover {
    background: var(--bg-hover);
    
    .news-title {
      color: var(--primary-600);
    }
    
    .link-icon {
      opacity: 1;
    }
  }
}

.news-rank {
  flex-shrink: 0;
  width: 22px;
  height: 22px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: 600;
  color: var(--text-tertiary);
  background: var(--bg-muted);
  border-radius: 6px;
  
  &.top {
    background: linear-gradient(135deg, #ff6b6b, #ff8e53);
    color: #fff;
  }
}

.news-content {
  flex: 1;
  min-width: 0;
}

.news-title {
  display: block;
  font-size: 14px;
  color: var(--text-primary);
  line-height: 1.5;
  margin-bottom: 4px;
  transition: color var(--transition-fast);
  
  // 限制两行
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.news-meta {
  display: flex;
  align-items: center;
  gap: 8px;
}

.news-hot {
  display: flex;
  align-items: center;
  gap: 3px;
  font-size: 11px;
  color: #ff6b6b;
  
  .hot-icon {
    width: 12px;
    height: 12px;
  }
}

.news-label {
  font-size: 11px;
  color: var(--text-tertiary);
  padding: 1px 6px;
  background: var(--bg-muted);
  border-radius: 4px;
}

.news-tag {
  font-size: 10px;
  padding: 1px 5px;
  border-radius: 3px;
  
  &.hot {
    background: rgba(255, 107, 107, 0.15);
    color: #ff6b6b;
  }
  
  &.new {
    background: rgba(52, 211, 153, 0.15);
    color: #34d399;
  }
}

.link-icon {
  flex-shrink: 0;
  width: 14px;
  height: 14px;
  color: var(--text-tertiary);
  opacity: 0;
  transition: opacity var(--transition-fast);
}

// ==================== 响应式 ====================

@media (max-width: 768px) {
  .source-tabs {
    :deep(.el-tabs__item) {
      padding: 0 10px;
      font-size: 12px;
    }
  }
  
  .news-item {
    padding: 10px 16px;
  }
  
  .news-title {
    font-size: 13px;
  }
}
</style>
