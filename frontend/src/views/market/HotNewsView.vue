<script setup lang="ts">
/**
 * 热点追踪页面
 * 
 * 参考 NewsNow 设计风格:
 * - 顶部筛选面板（关注、最热、实时）
 * - 用户自选来源
 * - 卡片拖拽排序
 * - 本地保存偏好
 */

import { ref, computed, onMounted, watch } from 'vue'
import { ElMessage, ElSkeleton, ElDialog, ElMessageBox } from 'element-plus'
import { marketApi } from '@/api'
import type { HotNewsItem } from '@/api/modules/market'
import { useUserStore } from '@/stores'

const userStore = useUserStore()

// ==================== 来源配置 ====================

interface SourceConfig {
  id: string
  name: string
  color: string
  title?: string
  type: 'hottest' | 'realtime'  // 最热 or 实时
}

const allSources: SourceConfig[] = [
  // 金融类
  { id: 'cls', name: '财联社', color: '#E53935', title: '电报', type: 'realtime' },
  { id: 'xueqiu', name: '雪球', color: '#1E88E5', title: '热门股票', type: 'hottest' },
  { id: 'wallstreetcn', name: '华尔街见闻', color: '#1976D2', title: '快讯', type: 'realtime' },
  { id: 'gelonghui', name: '格隆汇', color: '#1565C0', title: '事件', type: 'realtime' },
  { id: 'jin10', name: '金十数据', color: '#0D47A1', title: '快讯', type: 'realtime' },
  // 科技类
  { id: 'juejin', name: '稀土掘金', color: '#1E80FF', title: '热榜', type: 'hottest' },
  { id: 'ithome', name: 'IT之家', color: '#D32F2F', title: '最新', type: 'realtime' },
  { id: '36kr', name: '36氪', color: '#0080FF', title: '快讯', type: 'realtime' },
  { id: 'github', name: 'Github', color: '#24292E', title: 'Trending', type: 'hottest' },
  // 娱乐类
  { id: 'douyin', name: '抖音', color: '#000000', title: '热榜', type: 'hottest' },
  { id: 'bilibili', name: '哔哩哔哩', color: '#00A1D6', title: '热搜', type: 'hottest' },
  // 综合
  { id: 'kaopu', name: '靠谱新闻', color: '#607D8B', title: '国际', type: 'realtime' },
  { id: 'thepaper', name: '澎湃新闻', color: '#455A64', title: '热榜', type: 'hottest' },
]

// ==================== 状态 ====================

const STORAGE_KEY = 'hot_news_preferences'

// 当前视图模式: focus(关注) | hottest(最热) | realtime(实时)
const viewMode = ref<'focus' | 'hottest' | 'realtime'>('focus')

// 用户关注的来源ID列表（有序）
const focusedSources = ref<string[]>([])

// 新闻数据
const loading = ref(true)
const refreshingSource = ref<string | null>(null)
const newsMap = ref<Record<string, { news: HotNewsItem[], updated_at: string }>>({})

// 管理弹窗
const showSourceManager = ref(false)

// 采集状态（管理员专用）
const isCrawling = ref(false)

// 拖拽状态
const draggedIndex = ref<number | null>(null)
const dragOverIndex = ref<number | null>(null)

// 是否为管理员
const isAdmin = computed(() => userStore.isAdmin)

// ==================== 计算属性 ====================

// 当前显示的来源列表
const displaySources = computed(() => {
  if (viewMode.value === 'focus') {
    // 关注模式：按用户排序
    return focusedSources.value
      .map(id => allSources.find(s => s.id === id))
      .filter(Boolean) as SourceConfig[]
  } else if (viewMode.value === 'hottest') {
    // 最热模式
    return allSources.filter(s => s.type === 'hottest')
  } else {
    // 实时模式
    return allSources.filter(s => s.type === 'realtime')
  }
})

// ==================== 方法 ====================

// 加载用户偏好
function loadPreferences() {
  try {
    const saved = localStorage.getItem(STORAGE_KEY)
    if (saved) {
      const prefs = JSON.parse(saved)
      focusedSources.value = prefs.focusedSources || []
      viewMode.value = prefs.viewMode || 'focus'
    } else {
      // 默认关注前 6 个
      focusedSources.value = allSources.slice(0, 6).map(s => s.id)
    }
  } catch (e) {
    console.error('Failed to load preferences:', e)
    focusedSources.value = allSources.slice(0, 6).map(s => s.id)
  }
}

// 保存用户偏好
function savePreferences() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      focusedSources: focusedSources.value,
      viewMode: viewMode.value,
    }))
  } catch (e) {
    console.error('Failed to save preferences:', e)
  }
}

// 监听偏好变化并保存
watch([focusedSources, viewMode], () => {
  savePreferences()
}, { deep: true })

// 加载新闻数据
async function loadAllNews() {
  loading.value = true
  
  try {
    const sourcesToLoad = displaySources.value
    
    const promises = sourcesToLoad.map(async (source) => {
      try {
        const res = await marketApi.getHotNews(source.id, 30)
        return { source: source.id, data: { news: res.news, updated_at: res.updated_at || '' } }
      } catch (e) {
        console.error(`Failed to load ${source.id}:`, e)
        return { source: source.id, data: { news: [], updated_at: '' } }
      }
    })
    
    const results = await Promise.all(promises)
    
    results.forEach(({ source, data }) => {
      newsMap.value[source] = data
    })
  } finally {
    loading.value = false
  }
}

// 刷新单个来源（仅从 Redis 获取缓存，不触发 RPC 采集）
async function refreshSource(sourceId: string) {
  refreshingSource.value = sourceId
  
  try {
    // 只获取 Redis 缓存，不调用 RPC 重新采集
    const res = await marketApi.getHotNews(sourceId, 30)
    newsMap.value[sourceId] = { news: res.news, updated_at: res.updated_at || '' }
    ElMessage.success('已获取最新缓存')
  } catch (e) {
    console.error(`Failed to refresh ${sourceId}:`, e)
    ElMessage.error('刷新失败')
  } finally {
    refreshingSource.value = null
  }
}

// 采集全部数据（管理员专用）
async function crawlAllSources() {
  if (!isAdmin.value) return
  
  try {
    await ElMessageBox.confirm(
      '确定要重新采集所有热点新闻数据吗？这可能需要一些时间。',
      '采集确认',
      {
        confirmButtonText: '开始采集',
        cancelButtonText: '取消',
        type: 'info',
      }
    )
  } catch {
    return // 用户取消
  }
  
  isCrawling.value = true
  ElMessage.info('正在采集数据，请稍候...')
  
  try {
    // 调用后端刷新全部接口
    const result = await marketApi.refreshHotNews()
    
    ElMessage.success(
      `采集完成！成功: ${result.success_count || 0}, 失败: ${result.fail_count || 0}, 新闻总数: ${result.total_news || 0}`
    )
    
    // 重新加载数据
    await loadAllNews()
  } catch (e: any) {
    console.error('Crawl failed:', e)
    ElMessage.error('采集失败: ' + (e.message || '未知错误'))
  } finally {
    isCrawling.value = false
  }
}

// 切换关注状态
function toggleFocus(sourceId: string) {
  const idx = focusedSources.value.indexOf(sourceId)
  if (idx >= 0) {
    focusedSources.value.splice(idx, 1)
  } else {
    focusedSources.value.push(sourceId)
  }
}

// 是否已关注
function isFocused(sourceId: string) {
  return focusedSources.value.includes(sourceId)
}

// 格式化时间
function formatTime(time: string) {
  if (!time) return '加载中...'
  const date = new Date(time)
  const now = new Date()
  const diff = now.getTime() - date.getTime()
  
  if (diff < 60000) return '刚刚更新'
  if (diff < 3600000) return `${Math.floor(diff / 60000)}分钟前更新`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}小时前更新`
  return `${date.getMonth() + 1}/${date.getDate()}`
}

// 打开链接
function openLink(url: string) {
  if (url) {
    window.open(url, '_blank')
  }
}

// ==================== 拖拽排序 ====================

function onDragStart(index: number) {
  draggedIndex.value = index
}

function onDragOver(e: DragEvent, index: number) {
  e.preventDefault()
  dragOverIndex.value = index
}

function onDragEnd() {
  if (draggedIndex.value !== null && dragOverIndex.value !== null && draggedIndex.value !== dragOverIndex.value) {
    const sources = [...focusedSources.value]
    const [removed] = sources.splice(draggedIndex.value, 1)
    sources.splice(dragOverIndex.value, 0, removed)
    focusedSources.value = sources
  }
  draggedIndex.value = null
  dragOverIndex.value = null
}

function onDragLeave() {
  dragOverIndex.value = null
}

// ==================== 生命周期 ====================

onMounted(() => {
  loadPreferences()
  loadAllNews()
})

// 切换视图模式时重新加载
watch(viewMode, () => {
  loadAllNews()
})
</script>

<template>
  <div class="hot-news-page">
    <!-- 顶部导航栏 -->
    <div class="top-bar">
      <div class="nav-tabs">
        <button 
          class="nav-tab" 
          :class="{ active: viewMode === 'focus' }"
          @click="viewMode = 'focus'"
        >
          关注
        </button>
        <button 
          class="nav-tab" 
          :class="{ active: viewMode === 'hottest' }"
          @click="viewMode = 'hottest'"
        >
          最热
        </button>
        <button 
          class="nav-tab" 
          :class="{ active: viewMode === 'realtime' }"
          @click="viewMode = 'realtime'"
        >
          实时
        </button>
      </div>
      
      <div class="top-actions">
        <!-- 管理员专用：采集全部 -->
        <button 
          v-if="isAdmin" 
          class="crawl-btn" 
          @click="crawlAllSources" 
          :disabled="isCrawling"
        >
          <span class="icon" :class="{ spinning: isCrawling }">⟳</span>
          {{ isCrawling ? '采集中...' : '采集全部' }}
        </button>
        
        <button class="action-btn" @click="loadAllNews" :disabled="loading" title="获取最新缓存（数据每5分钟自动更新）">
          <span class="icon" :class="{ spinning: loading }">↻</span>
        </button>
        <button class="action-btn" @click="showSourceManager = true" title="管理来源">
          <span class="icon">⚙</span>
        </button>
      </div>
    </div>
    
    <!-- 卡片网格 -->
    <div class="cards-grid">
      <div
        v-for="(source, index) in displaySources"
        :key="source.id"
        class="news-card"
        :style="{ '--card-color': source.color }"
        :draggable="viewMode === 'focus'"
        :class="{ 
          dragging: draggedIndex === index,
          'drag-over': dragOverIndex === index 
        }"
        @dragstart="onDragStart(index)"
        @dragover="onDragOver($event, index)"
        @dragend="onDragEnd"
        @dragleave="onDragLeave"
      >
        <!-- 卡片头部 -->
        <div class="card-header">
          <div class="header-info">
            <span class="source-name">{{ source.name }}</span>
            <span v-if="source.title" class="source-tag">{{ source.title }}</span>
          </div>
          <div class="header-actions">
            <button 
              class="action-btn refresh"
              @click="refreshSource(source.id)"
              :disabled="refreshingSource === source.id"
              :title="'获取最新缓存'"
            >
              <span :class="{ spinning: refreshingSource === source.id }">↻</span>
            </button>
            <button 
              class="action-btn star"
              :class="{ active: isFocused(source.id) }"
              @click="toggleFocus(source.id)"
              :title="isFocused(source.id) ? '取消关注' : '关注'"
            >
              {{ isFocused(source.id) ? '★' : '☆' }}
            </button>
            <div v-if="viewMode === 'focus'" class="drag-handle" title="拖拽排序">⋮⋮</div>
          </div>
        </div>
        
        <!-- 更新时间 -->
        <div class="update-time">{{ formatTime(newsMap[source.id]?.updated_at) }}</div>
        
        <!-- 卡片内容 -->
        <div class="card-content">
          <ElSkeleton v-if="loading && !newsMap[source.id]?.news?.length" :rows="8" animated />
          
          <div v-else-if="!newsMap[source.id]?.news?.length" class="empty-state">
            暂无数据
          </div>
          
          <ol v-else class="news-list">
            <li
              v-for="(news, idx) in newsMap[source.id]?.news || []"
              :key="`${source.id}-${idx}`"
              class="news-item"
              @click="openLink(news.url)"
            >
              <span class="item-rank" :class="{ top: idx < 3 }">{{ idx + 1 }}</span>
              <span class="item-title">{{ news.title }}</span>
              <!-- 雪球股票涨跌幅 -->
              <span 
                v-if="news.extra?.percent" 
                class="item-percent" 
                :class="{ up: parseFloat(news.extra.percent) > 0, down: parseFloat(news.extra.percent) < 0 }"
              >
                {{ news.extra.percent }}
              </span>
              <!-- 靠谱新闻来源 -->
              <span v-if="news.extra?.publisher" class="item-publisher">
                {{ news.extra.publisher }}
              </span>
              <!-- 金十重要标记 -->
              <span v-if="news.extra?.important" class="item-important">✰</span>
            </li>
          </ol>
        </div>
      </div>
      
      <!-- 空状态 -->
      <div v-if="displaySources.length === 0 && viewMode === 'focus'" class="empty-focus">
        <p>暂无关注的来源</p>
        <button class="add-btn" @click="showSourceManager = true">+ 添加关注</button>
      </div>
    </div>
    
    <!-- 来源管理弹窗 -->
    <ElDialog 
      v-model="showSourceManager" 
      title="管理关注来源" 
      width="500px"
      :close-on-click-modal="true"
    >
      <div class="source-manager">
        <p class="manager-tip">选择你想关注的新闻来源，拖拽卡片可调整顺序</p>
        <div class="source-list">
          <div 
            v-for="source in allSources" 
            :key="source.id"
            class="source-item"
            :class="{ selected: isFocused(source.id) }"
            @click="toggleFocus(source.id)"
          >
            <span class="source-color" :style="{ background: source.color }"></span>
            <span class="source-label">{{ source.name }}</span>
            <span class="source-type">{{ source.title }}</span>
            <span class="source-check">{{ isFocused(source.id) ? '✓' : '' }}</span>
          </div>
        </div>
      </div>
    </ElDialog>
  </div>
</template>

<style scoped lang="scss">
.hot-news-page {
  min-height: 100vh;
  padding: 16px 20px;
  background: var(--bg-base);
}

// ==================== 顶部导航栏 ====================

.top-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  padding: 0 4px;
}

.nav-tabs {
  display: flex;
  gap: 8px;
  background: var(--bg-elevated);
  padding: 4px;
  border-radius: 8px;
}

.nav-tab {
  padding: 8px 20px;
  background: transparent;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    color: var(--text-primary);
  }
  
  &.active {
    background: var(--primary-500);
    color: #fff;
  }
}

.top-actions {
  display: flex;
  gap: 8px;
  align-items: center;
}

.crawl-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  background: linear-gradient(135deg, #FF6B35, #F7931E);
  color: #fff;
  border: none;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  
  .icon {
    font-size: 14px;
  }
  
  &:hover {
    background: linear-gradient(135deg, #E55A2B, #E0850E);
    transform: translateY(-1px);
  }
  
  &:disabled {
    opacity: 0.7;
    cursor: not-allowed;
    transform: none;
  }
}

.top-actions .action-btn {
  width: 36px;
  height: 36px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-elevated);
  border: 1px solid var(--border-base);
  border-radius: 8px;
  font-size: 16px;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-muted);
    color: var(--text-primary);
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
}

// ==================== 卡片网格 ====================

.cards-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
  gap: 16px;
}

.news-card {
  display: flex;
  flex-direction: column;
  height: 500px;
  background: color-mix(in srgb, var(--card-color) 15%, var(--bg-elevated));
  border-radius: 16px;
  overflow: hidden;
  transition: transform 0.2s, opacity 0.2s;
  border: 1px solid color-mix(in srgb, var(--card-color) 25%, transparent);
  
  &:hover {
    transform: translateY(-2px);
  }
  
  &.dragging {
    opacity: 0.5;
    cursor: grabbing;
  }
  
  &.drag-over {
    border-color: var(--primary-500);
    box-shadow: 0 0 0 2px var(--primary-500);
  }
}

// ==================== 卡片头部 ====================

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 16px;
  background: color-mix(in srgb, var(--card-color) 25%, transparent);
}

.header-info {
  display: flex;
  align-items: center;
  gap: 8px;
}

.source-name {
  font-size: 16px;
  font-weight: 700;
  color: var(--text-primary);
}

.source-tag {
  font-size: 11px;
  padding: 2px 6px;
  background: color-mix(in srgb, var(--card-color) 30%, var(--bg-base));
  color: var(--card-color);
  border-radius: 4px;
  font-weight: 500;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 6px;
}

.header-actions .action-btn {
  width: 26px;
  height: 26px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: color-mix(in srgb, var(--card-color) 15%, transparent);
  border: none;
  border-radius: 6px;
  color: var(--text-secondary);
  font-size: 14px;
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: color-mix(in srgb, var(--card-color) 30%, transparent);
    color: var(--card-color);
  }
  
  &.star.active {
    color: #FFB300;
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
}

.drag-handle {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 26px;
  color: var(--text-tertiary);
  cursor: grab;
  font-size: 12px;
  letter-spacing: -2px;
  
  &:hover {
    color: var(--text-secondary);
  }
}

.update-time {
  padding: 0 16px 8px;
  font-size: 11px;
  color: var(--text-tertiary);
}

// ==================== 卡片内容 ====================

.card-content {
  flex: 1;
  overflow-y: auto;
  padding: 8px;
  background: color-mix(in srgb, var(--bg-base) 90%, var(--card-color));
  border-radius: 12px;
  margin: 0 8px 8px;
  
  &::-webkit-scrollbar {
    width: 5px;
  }
  
  &::-webkit-scrollbar-track {
    background: transparent;
  }
  
  &::-webkit-scrollbar-thumb {
    background: color-mix(in srgb, var(--card-color) 25%, transparent);
    border-radius: 3px;
  }
}

.empty-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--text-tertiary);
  font-size: 14px;
}

.news-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  list-style: none;
  margin: 0;
  padding: 0;
}

.news-item {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 6px 8px;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.15s;
  
  &:hover {
    background: color-mix(in srgb, var(--card-color) 12%, transparent);
    
    .item-title {
      color: var(--card-color);
    }
  }
}

.item-rank {
  flex-shrink: 0;
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 11px;
  font-weight: 600;
  color: var(--text-tertiary);
  background: color-mix(in srgb, var(--card-color) 10%, var(--bg-muted));
  border-radius: 5px;
  
  &.top {
    background: var(--card-color);
    color: #fff;
  }
}

.item-title {
  flex: 1;
  font-size: 13px;
  line-height: 1.4;
  color: var(--text-primary);
  transition: color 0.15s;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.item-percent {
  flex-shrink: 0;
  font-size: 12px;
  font-weight: 600;
  font-family: 'SF Mono', 'Consolas', monospace;
  
  &.up { color: #F23645; }
  &.down { color: #089981; }
}

.item-publisher {
  flex-shrink: 0;
  font-size: 10px;
  color: var(--text-tertiary);
  max-width: 60px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.item-important {
  color: #FFB300;
  font-size: 12px;
}

// ==================== 空状态 ====================

.empty-focus {
  grid-column: 1 / -1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 80px 20px;
  color: var(--text-tertiary);
  
  p {
    margin-bottom: 16px;
    font-size: 16px;
  }
}

.add-btn {
  padding: 10px 24px;
  background: var(--primary-500);
  color: #fff;
  border: none;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: background 0.2s;
  
  &:hover {
    background: var(--primary-600);
  }
}

// ==================== 来源管理弹窗 ====================

.source-manager {
  padding: 8px 0;
}

.manager-tip {
  margin: 0 0 16px;
  font-size: 13px;
  color: var(--text-tertiary);
}

.source-list {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 8px;
}

.source-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  background: var(--bg-muted);
  border: 1px solid var(--border-base);
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-elevated);
  }
  
  &.selected {
    border-color: var(--primary-500);
    background: color-mix(in srgb, var(--primary-500) 10%, var(--bg-elevated));
  }
}

.source-color {
  width: 12px;
  height: 12px;
  border-radius: 3px;
  flex-shrink: 0;
}

.source-label {
  flex: 1;
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
}

.source-type {
  font-size: 11px;
  color: var(--text-tertiary);
}

.source-check {
  width: 16px;
  font-size: 14px;
  color: var(--primary-500);
  font-weight: bold;
}

// ==================== 动画 ====================

.spinning {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

// ==================== 响应式 ====================

@media (max-width: 768px) {
  .hot-news-page {
    padding: 12px;
  }
  
  .cards-grid {
    grid-template-columns: 1fr;
  }
  
  .news-card {
    height: 400px;
  }
  
  .source-list {
    grid-template-columns: 1fr;
  }
}
</style>
