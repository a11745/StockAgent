<script setup lang="ts">
/**
 * 报告回顾页面
 * 
 * 支持：
 * - 按日期筛选
 * - 报告类型筛选（早报/午报）
 * - 分页浏览
 * - 查看详情
 */

import { ref, computed, onMounted, watch } from 'vue'
import { 
  ElMessage, 
  ElSkeleton, 
  ElDialog, 
  ElDatePicker, 
  ElPagination, 
  ElEmpty,
  ElTag,
  ElCollapse,
  ElCollapseItem,
} from 'element-plus'
import { reportApi } from '@/api'
import type { ReportListItem, ReportDetail, ReportSection } from '@/api/modules/report'

// ==================== 状态 ====================

const loading = ref(true)
const loadingDetail = ref(false)

// 筛选条件
const selectedDate = ref<string | null>(null)
const reportType = ref<'all' | 'morning' | 'noon'>('all')

// 分页
const page = ref(1)
const pageSize = ref(10)
const total = ref(0)

// 数据
const reports = ref<ReportListItem[]>([])
const availableDates = ref<string[]>([])

// 详情弹窗
const showDetail = ref(false)
const currentReport = ref<ReportDetail | null>(null)

// ==================== 计算属性 ====================

const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

const typeLabel = computed(() => ({
  morning: '早报',
  noon: '午报',
}))

const typeColor = computed(() => ({
  morning: '#FF9800',
  noon: '#2196F3',
}))

// ==================== 方法 ====================

async function loadReports() {
  loading.value = true
  
  try {
    const params: any = {
      page: page.value,
      page_size: pageSize.value,
    }
    
    if (selectedDate.value) {
      params.date = selectedDate.value
    }
    
    if (reportType.value !== 'all') {
      params.report_type = reportType.value
    }
    
    const res = await reportApi.getReports(params)
    reports.value = res.items
    total.value = res.total
  } catch (e) {
    console.error('Failed to load reports:', e)
    ElMessage.error('加载报告列表失败')
  } finally {
    loading.value = false
  }
}

async function loadAvailableDates() {
  try {
    const res = await reportApi.getReportDates(60)
    availableDates.value = res.dates
  } catch (e) {
    console.error('Failed to load dates:', e)
  }
}

async function viewReport(reportId: string) {
  showDetail.value = true
  loadingDetail.value = true
  currentReport.value = null
  
  try {
    const res = await reportApi.getReportDetail(reportId)
    currentReport.value = res
  } catch (e) {
    console.error('Failed to load report detail:', e)
    ElMessage.error('加载报告详情失败')
  } finally {
    loadingDetail.value = false
  }
}

function formatDate(dateStr: string) {
  if (!dateStr) return ''
  const date = new Date(dateStr)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function formatReportDate(dateStr: string) {
  if (!dateStr) return ''
  const parts = dateStr.split('-')
  if (parts.length !== 3) return dateStr
  return `${parts[1]}月${parts[2]}日`
}

function getImportanceIcon(importance: string) {
  switch (importance) {
    case 'high': return '🔴'
    case 'medium': return '🟡'
    case 'low': return '🟢'
    default: return '⚪'
  }
}

function clearDateFilter() {
  selectedDate.value = null
  page.value = 1
  loadReports()
}

function handlePageChange(newPage: number) {
  page.value = newPage
  loadReports()
}

function disabledDate(date: Date) {
  const dateStr = date.toISOString().split('T')[0]
  return !availableDates.value.includes(dateStr)
}

// ==================== 监听 ====================

watch([selectedDate, reportType], () => {
  page.value = 1
  loadReports()
})

// ==================== 生命周期 ====================

onMounted(() => {
  loadReports()
  loadAvailableDates()
})
</script>

<template>
  <div class="report-page">
    <!-- 顶部筛选栏 -->
    <div class="filter-bar">
      <div class="filter-left">
        <!-- 日期选择 -->
        <div class="filter-item">
          <span class="filter-label">日期：</span>
          <ElDatePicker
            v-model="selectedDate"
            type="date"
            placeholder="全部日期"
            format="YYYY-MM-DD"
            value-format="YYYY-MM-DD"
            :disabled-date="disabledDate"
            clearable
            size="default"
            style="width: 160px"
          />
          <button 
            v-if="selectedDate" 
            class="clear-btn"
            @click="clearDateFilter"
          >
            查看全部
          </button>
        </div>
        
        <!-- 类型筛选 -->
        <div class="filter-item">
          <span class="filter-label">类型：</span>
          <div class="type-tabs">
            <button 
              class="type-tab" 
              :class="{ active: reportType === 'all' }"
              @click="reportType = 'all'"
            >
              全部
            </button>
            <button 
              class="type-tab morning" 
              :class="{ active: reportType === 'morning' }"
              @click="reportType = 'morning'"
            >
              ☀️ 早报
            </button>
            <button 
              class="type-tab noon" 
              :class="{ active: reportType === 'noon' }"
              @click="reportType = 'noon'"
            >
              🌞 午报
            </button>
          </div>
        </div>
      </div>
      
      <div class="filter-right">
        <span class="total-count">共 {{ total }} 份报告</span>
      </div>
    </div>
    
    <!-- 报告列表 -->
    <div class="report-list">
      <ElSkeleton v-if="loading" :rows="8" animated />
      
      <ElEmpty v-else-if="reports.length === 0" description="暂无报告" />
      
      <div v-else class="report-cards">
        <div 
          v-for="report in reports" 
          :key="report.id"
          class="report-card"
          :class="report.type"
          @click="viewReport(report.id)"
        >
          <div class="card-header">
            <div class="header-left">
              <ElTag 
                :color="typeColor[report.type]" 
                effect="dark" 
                size="small"
                class="type-tag"
              >
                {{ report.type === 'morning' ? '☀️ 早报' : '🌞 午报' }}
              </ElTag>
              <span class="report-date">{{ formatReportDate(report.date) }}</span>
            </div>
            <div class="header-right">
              <span class="created-time">{{ formatDate(report.created_at) }}</span>
            </div>
          </div>
          
          <h3 class="report-title">{{ report.title }}</h3>
          
          <p v-if="report.overview" class="report-overview">
            {{ report.overview }}
          </p>
          
          <div class="report-stats">
            <div class="stat-item">
              <span class="stat-value">{{ report.stats.event_count }}</span>
              <span class="stat-label">事件</span>
            </div>
            <div class="stat-item">
              <span class="stat-value">{{ report.stats.news_count }}</span>
              <span class="stat-label">新闻</span>
            </div>
            <div class="stat-item highlight">
              <span class="stat-value">{{ report.stats.high_importance_count }}</span>
              <span class="stat-label">重要</span>
            </div>
            <div class="stat-divider"></div>
            <div class="stat-item macro">
              <span class="stat-value">{{ report.stats.macro_count }}</span>
              <span class="stat-label">宏观</span>
            </div>
            <div class="stat-item industry">
              <span class="stat-value">{{ report.stats.industry_count }}</span>
              <span class="stat-label">行业</span>
            </div>
            <div class="stat-item stock">
              <span class="stat-value">{{ report.stats.stock_count }}</span>
              <span class="stat-label">个股</span>
            </div>
            <div class="stat-item hot">
              <span class="stat-value">{{ report.stats.hot_count }}</span>
              <span class="stat-label">热点</span>
            </div>
          </div>
          
          <div class="card-footer">
            <div class="push-status">
              <span v-if="report.pushed.wechat" class="push-badge wechat">✓ 企微</span>
              <span v-if="report.pushed.websocket" class="push-badge ws">✓ 推送</span>
            </div>
            <span class="view-detail">查看详情 →</span>
          </div>
        </div>
      </div>
    </div>
    
    <!-- 分页 -->
    <div v-if="total > pageSize" class="pagination-wrapper">
      <ElPagination
        v-model:current-page="page"
        :page-size="pageSize"
        :total="total"
        layout="prev, pager, next, jumper"
        @current-change="handlePageChange"
      />
    </div>
    
    <!-- 详情弹窗 -->
    <ElDialog 
      v-model="showDetail" 
      :title="currentReport?.title || '报告详情'"
      width="800px"
      top="5vh"
      class="report-detail-dialog"
    >
      <div v-if="loadingDetail" class="detail-loading">
        <ElSkeleton :rows="12" animated />
      </div>
      
      <div v-else-if="currentReport" class="detail-content">
        <!-- 概述 -->
        <div v-if="currentReport.overview" class="detail-overview">
          <h4>📋 概述</h4>
          <p>{{ currentReport.overview }}</p>
        </div>
        
        <!-- 分节内容 -->
        <ElCollapse v-if="currentReport.sections.length > 0">
          <ElCollapseItem 
            v-for="(section, idx) in currentReport.sections" 
            :key="idx"
            :name="idx"
          >
            <template #title>
              <div class="section-title">
                <span>{{ section.title }}</span>
                <ElTag size="small" type="info">{{ section.item_count }} 条</ElTag>
              </div>
            </template>
            
            <div v-if="section.summary" class="section-summary">
              {{ section.summary }}
            </div>
            
            <div class="section-items">
              <div 
                v-for="item in section.items" 
                :key="item.event_id"
                class="event-item"
              >
                <span class="importance-icon">{{ getImportanceIcon(item.importance) }}</span>
                <div class="event-content">
                  <div class="event-title">{{ item.title }}</div>
                  <div v-if="item.summary" class="event-summary">{{ item.summary }}</div>
                  <div class="event-meta">
                    <span v-if="item.news_count > 1" class="meta-item">
                      📰 {{ item.news_count }} 篇相关
                    </span>
                    <span v-if="item.ts_codes.length > 0" class="meta-item codes">
                      🏷️ {{ item.ts_codes.slice(0, 3).join(', ') }}
                      <span v-if="item.ts_codes.length > 3">等</span>
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </ElCollapseItem>
        </ElCollapse>
        
        <ElEmpty v-else description="暂无内容" />
        
        <!-- Markdown 原文 -->
        <details v-if="currentReport.content_markdown" class="markdown-section">
          <summary>📄 查看 Markdown 原文</summary>
          <pre class="markdown-content">{{ currentReport.content_markdown }}</pre>
        </details>
      </div>
    </ElDialog>
  </div>
</template>

<style scoped lang="scss">
.report-page {
  min-height: 100vh;
  padding: 20px 24px;
  background: var(--bg-base);
}

// ==================== 筛选栏 ====================

.filter-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
  padding: 16px 20px;
  background: var(--bg-elevated);
  border-radius: 12px;
  border: 1px solid var(--border-base);
}

.filter-left {
  display: flex;
  align-items: center;
  gap: 24px;
}

.filter-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.filter-label {
  font-size: 14px;
  color: var(--text-secondary);
  font-weight: 500;
}

.clear-btn {
  padding: 6px 12px;
  background: var(--bg-muted);
  border: 1px solid var(--border-base);
  border-radius: 6px;
  font-size: 13px;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    background: var(--bg-hover);
    color: var(--primary-500);
    border-color: var(--primary-500);
  }
}

.type-tabs {
  display: flex;
  gap: 4px;
  background: var(--bg-muted);
  padding: 4px;
  border-radius: 8px;
}

.type-tab {
  padding: 6px 14px;
  background: transparent;
  border: none;
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
    background: var(--bg-elevated);
    color: var(--text-primary);
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }
  
  &.morning.active {
    background: #FFF3E0;
    color: #E65100;
  }
  
  &.noon.active {
    background: #E3F2FD;
    color: #1565C0;
  }
}

.filter-right {
  .total-count {
    font-size: 14px;
    color: var(--text-tertiary);
  }
}

// ==================== 报告列表 ====================

.report-list {
  min-height: 400px;
}

.report-cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 16px;
}

.report-card {
  background: var(--bg-elevated);
  border-radius: 12px;
  padding: 16px 20px;
  border: 1px solid var(--border-base);
  cursor: pointer;
  transition: all 0.2s;
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08);
    border-color: var(--primary-300);
  }
  
  &.morning {
    border-left: 4px solid #FF9800;
  }
  
  &.noon {
    border-left: 4px solid #2196F3;
  }
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 10px;
}

.type-tag {
  font-size: 12px;
}

.report-date {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
}

.header-right {
  .created-time {
    font-size: 12px;
    color: var(--text-tertiary);
  }
}

.report-title {
  margin: 0 0 10px;
  font-size: 16px;
  font-weight: 600;
  color: var(--text-primary);
  line-height: 1.4;
}

.report-overview {
  margin: 0 0 14px;
  font-size: 13px;
  color: var(--text-secondary);
  line-height: 1.5;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.report-stats {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 0;
  border-top: 1px solid var(--border-base);
  border-bottom: 1px solid var(--border-base);
  margin-bottom: 12px;
}

.stat-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 2px;
  min-width: 40px;
  
  .stat-value {
    font-size: 18px;
    font-weight: 700;
    color: var(--text-primary);
  }
  
  .stat-label {
    font-size: 11px;
    color: var(--text-tertiary);
  }
  
  &.highlight .stat-value {
    color: #F44336;
  }
  
  &.macro .stat-value {
    color: #9C27B0;
  }
  
  &.industry .stat-value {
    color: #2196F3;
  }
  
  &.stock .stat-value {
    color: #4CAF50;
  }
  
  &.hot .stat-value {
    color: #FF5722;
  }
}

.stat-divider {
  width: 1px;
  height: 30px;
  background: var(--border-base);
  margin: 0 4px;
}

.card-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.push-status {
  display: flex;
  gap: 6px;
}

.push-badge {
  font-size: 11px;
  padding: 2px 8px;
  border-radius: 4px;
  
  &.wechat {
    background: #E8F5E9;
    color: #2E7D32;
  }
  
  &.ws {
    background: #E3F2FD;
    color: #1565C0;
  }
}

.view-detail {
  font-size: 13px;
  color: var(--primary-500);
  font-weight: 500;
}

// ==================== 分页 ====================

.pagination-wrapper {
  display: flex;
  justify-content: center;
  margin-top: 24px;
  padding: 16px;
}

// ==================== 详情弹窗 ====================

.detail-loading {
  padding: 20px;
}

.detail-content {
  max-height: 70vh;
  overflow-y: auto;
  padding: 0 4px;
}

.detail-overview {
  margin-bottom: 20px;
  padding: 16px;
  background: var(--bg-muted);
  border-radius: 8px;
  
  h4 {
    margin: 0 0 8px;
    font-size: 15px;
    font-weight: 600;
    color: var(--text-primary);
  }
  
  p {
    margin: 0;
    font-size: 14px;
    color: var(--text-secondary);
    line-height: 1.6;
  }
}

.section-title {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 15px;
  font-weight: 600;
}

.section-summary {
  padding: 12px;
  margin-bottom: 12px;
  background: var(--bg-muted);
  border-radius: 6px;
  font-size: 13px;
  color: var(--text-secondary);
  line-height: 1.5;
}

.section-items {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.event-item {
  display: flex;
  gap: 10px;
  padding: 12px;
  background: var(--bg-base);
  border-radius: 8px;
  border: 1px solid var(--border-base);
}

.importance-icon {
  flex-shrink: 0;
  font-size: 14px;
}

.event-content {
  flex: 1;
  min-width: 0;
}

.event-title {
  font-size: 14px;
  font-weight: 500;
  color: var(--text-primary);
  margin-bottom: 4px;
}

.event-summary {
  font-size: 13px;
  color: var(--text-secondary);
  line-height: 1.5;
  margin-bottom: 6px;
}

.event-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  font-size: 12px;
  color: var(--text-tertiary);
}

.meta-item {
  display: flex;
  align-items: center;
  gap: 4px;
  
  &.codes {
    color: var(--primary-500);
  }
}

.markdown-section {
  margin-top: 20px;
  
  summary {
    cursor: pointer;
    padding: 10px 0;
    font-size: 14px;
    color: var(--text-secondary);
    
    &:hover {
      color: var(--primary-500);
    }
  }
}

.markdown-content {
  margin-top: 10px;
  padding: 16px;
  background: var(--bg-muted);
  border-radius: 8px;
  font-size: 13px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 400px;
  overflow-y: auto;
}

// ==================== 响应式 ====================

@media (max-width: 768px) {
  .report-page {
    padding: 12px;
  }
  
  .filter-bar {
    flex-direction: column;
    gap: 12px;
    align-items: flex-start;
  }
  
  .filter-left {
    flex-direction: column;
    align-items: flex-start;
    width: 100%;
  }
  
  .report-cards {
    grid-template-columns: 1fr;
  }
  
  .report-stats {
    flex-wrap: wrap;
    justify-content: flex-start;
  }
  
  .stat-divider {
    display: none;
  }
}
</style>
