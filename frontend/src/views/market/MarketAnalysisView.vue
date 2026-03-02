<template>
  <div class="market-terminal">
    <!-- 顶部统计条 -->
    <div class="stats-strip" v-if="latestData">
      <div class="stat-block primary">
        <span class="stat-label">强度</span>
        <span class="stat-value">{{ latestData.scores.strength.toFixed(0) }}</span>
      </div>
      <div class="stat-block">
        <span class="stat-label">情绪</span>
        <span class="stat-value">{{ latestData.scores.sentiment.toFixed(0) }}</span>
      </div>
      <div class="stat-divider"></div>
      <div class="stat-block">
        <span class="stat-label">成交</span>
        <span class="stat-value">{{ formatAmount(latestData.stats.total_amount) }}</span>
      </div>
      <div class="stat-block up">
        <span class="stat-label">涨停</span>
        <span class="stat-value">{{ latestData.stats.limit_up_count }}</span>
      </div>
      <div class="stat-block down">
        <span class="stat-label">跌停</span>
        <span class="stat-value">{{ latestData.stats.limit_down_count }}</span>
      </div>
      <div class="stat-divider"></div>
      <div class="stat-block accent">
        <span class="stat-label">高度</span>
        <span class="stat-value">{{ latestData.stats.max_limit_height }}板</span>
      </div>
      <div class="stat-block" :class="latestData.stats.up_count > latestData.stats.down_count ? 'up' : 'down'">
        <span class="stat-label">涨/跌</span>
        <span class="stat-value">{{ latestData.stats.up_count }}/{{ latestData.stats.down_count }}</span>
      </div>
      <div class="stat-block" :class="(latestData.stats.north_money || 0) >= 0 ? 'up' : 'down'">
        <span class="stat-label">北向</span>
        <span class="stat-value">{{ formatNorth(latestData.stats.north_money) }}</span>
      </div>
      
      <!-- 周期标识 -->
      <div class="cycle-indicator" :class="cycleClass">
        <span class="cycle-pulse"></span>
        <span class="cycle-name">{{ cycleDisplayName }}</span>
      </div>
      
      <!-- 日期与刷新 -->
      <div class="header-actions">
        <span class="trade-date">{{ formatDate(latestData.trade_date) }}</span>
        <button class="refresh-btn" @click="refreshData" :disabled="loading">
          <svg viewBox="0 0 24 24" :class="{ spinning: loading }">
            <path fill="currentColor" d="M17.65 6.35A7.958 7.958 0 0012 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08A5.99 5.99 0 0112 18c-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z"/>
          </svg>
        </button>
      </div>
    </div>

    <!-- 主线雷达 -->
    <div class="theme-radar-strip" v-if="themeRadar && themeRadar.radar.length > 0">
      <div class="radar-title">
        <span class="radar-icon">◉</span>
        <span>主线雷达</span>
      </div>
      <div class="radar-items">
        <div 
          v-for="(item, idx) in themeRadar.radar" 
          :key="idx" 
          class="radar-item"
          :class="item.status"
        >
          <span class="radar-name">{{ item.name }}</span>
          <span class="radar-days">{{ item.consecutive_days }}天</span>
          <span class="radar-trend" :class="item.trend">
            {{ item.trend === 'up' ? '↑' : (item.trend === 'down' ? '↓' : '→') }}
            {{ Math.abs(item.rank_change) > 0 ? Math.abs(item.rank_change) : '' }}
          </span>
        </div>
      </div>
      <div class="rotation-hint" v-if="themeRadar.rotation.length > 0">
        <span class="rotation-label">轮动:</span>
        <span 
          v-for="(r, idx) in themeRadar.rotation.slice(0, 3)" 
          :key="idx"
          class="rotation-item"
          :class="r.trend"
        >
          {{ r.name }} {{ r.trend === 'rising' ? '↑' : '↓' }}{{ Math.abs(r.change) }}
        </span>
      </div>
    </div>

    <!-- 导航标签 -->
    <div class="nav-bar">
      <button 
        v-for="tab in tabs" 
        :key="tab.key" 
        :class="['nav-tab', { active: activeTab === tab.key }]"
        @click="activeTab = tab.key"
      >
        {{ tab.label }}
      </button>
    </div>

    <!-- 主图表区域 -->
    <div class="chart-zone" v-show="activeTab === 'market_data'">
      <!-- 核心趋势图 -->
      <div class="chart-container main-chart">
        <div class="chart-title">
          <span>市场强度 × 情绪评分</span>
          <div class="chart-legend">
            <span class="legend strength"><i></i>强度</span>
            <span class="legend sentiment"><i></i>情绪</span>
            <span class="legend bar-pos"><i></i>强弱差+</span>
            <span class="legend bar-neg"><i></i>强弱差-</span>
          </div>
        </div>
        <div ref="sentimentChartRef" class="chart-canvas main"></div>
      </div>
      
      <!-- 综合趋势图 -->
      <div class="chart-container sub-chart">
        <div class="chart-title">
          <span>综合趋势指标</span>
          <div class="chart-legend">
            <span class="legend" style="color: #60a5fa"><i style="background: #60a5fa"></i>成交量</span>
            <span class="legend" style="color: #f87171"><i style="background: #f87171"></i>涨停</span>
            <span class="legend" style="color: #4ade80"><i style="background: #4ade80"></i>跌停</span>
            <span class="legend" style="color: #fbbf24"><i style="background: #fbbf24"></i>天梯</span>
          </div>
        </div>
        <div ref="trendChartRef" class="chart-canvas" style="height: 400px;"></div>
      </div>

      <!-- 副图：涨跌停柱状图 -->
      <div class="chart-container sub-chart">
        <div class="chart-title"><span>涨跌停 / 连板高度</span></div>
        <div ref="limitChartRef" class="chart-canvas sub"></div>
      </div>
      
      <!-- 副图：涨跌停堆叠对比 -->
      <div class="chart-container sub-chart">
        <div class="chart-title"><span>涨跌停对比</span></div>
        <div ref="limitStackChartRef" class="chart-canvas sub"></div>
      </div>
      
      <!-- 副图：涨跌家数 -->
      <div class="chart-container sub-chart">
        <div class="chart-title"><span>涨跌家数</span></div>
        <div ref="upDownChartRef" class="chart-canvas sub"></div>
      </div>
    </div>

    <!-- 历史数据表 -->
    <div class="table-zone" v-show="activeTab === 'history_table'">
      <div class="table-wrapper">
        <table class="data-table">
          <thead>
            <tr>
              <th class="fixed-col">日期</th>
              <th>上涨</th>
              <th>下跌</th>
              <th>涨停</th>
              <th>跌停</th>
              <th>首板</th>
              <th>二板</th>
              <th>三板</th>
              <th>四板</th>
              <th>五板+</th>
              <th>高度</th>
              <th>强度</th>
              <th>情绪</th>
              <th>周期</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in tableData" :key="row.trade_date">
              <td class="fixed-col date-cell">{{ formatTableDate(row.trade_date) }}</td>
              <td class="up-cell">{{ row.up_count }}</td>
              <td class="down-cell">{{ row.down_count }}</td>
              <td class="up-cell bold">{{ row.limit_up_count }}</td>
              <td class="down-cell bold">{{ row.limit_down_count }}</td>
              <td>{{ row.limit_1 }}</td>
              <td>{{ row.limit_2 }}</td>
              <td>{{ row.limit_3 }}</td>
              <td>{{ row.limit_4 }}</td>
              <td>{{ (row.limit_5 || 0) + (row.limit_6_plus || 0) }}</td>
              <td class="height-cell">{{ row.max_limit_height }}</td>
              <td class="score-cell">{{ row.strength_score?.toFixed(0) ?? '-' }}</td>
              <td class="score-cell">{{ row.sentiment_score?.toFixed(0) ?? '-' }}</td>
              <td><span class="cycle-tag" :class="row.cycle">{{ getCycleShortName(row.cycle) }}</span></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 行业排行榜 - 时间轴表格 -->
    <div class="ranking-table-zone" v-show="activeTab === 'industry_ranking'">
      <div class="ranking-table-wrapper">
        <table class="ranking-timeline-table">
          <thead>
            <tr>
              <th class="rank-col">排名</th>
              <th v-for="h in industryHistory" :key="h.trade_date" class="date-col">
                {{ formatRankingDate(h.trade_date) }}
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="rank in 20" :key="rank">
              <td class="rank-col">{{ rank }}</td>
              <td v-for="h in industryHistory" :key="h.trade_date + rank" class="data-col">
                <template v-if="h.rankings[rank - 1]">
                  <div class="cell-name">{{ h.rankings[rank - 1].name || h.rankings[rank - 1].ts_code }}</div>
                  <div class="cell-pct" :class="h.rankings[rank - 1].pct_change >= 0 ? 'up' : 'down'">
                    {{ h.rankings[rank - 1].pct_change >= 0 ? '+' : '' }}{{ h.rankings[rank - 1].pct_change?.toFixed(1) }}%
                  </div>
                  <div class="cell-lead" v-if="h.rankings[rank - 1].lead_stock">
                    {{ h.rankings[rank - 1].lead_stock }}
                  </div>
                </template>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 板块排行榜 - 时间轴表格 -->
    <div class="ranking-table-zone" v-show="activeTab === 'concept_ranking'">
      <div class="ranking-table-wrapper">
        <table class="ranking-timeline-table">
          <thead>
            <tr>
              <th class="rank-col">排名</th>
              <th v-for="h in conceptHistory" :key="h.trade_date" class="date-col">
                {{ formatRankingDate(h.trade_date) }}
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="rank in 20" :key="rank">
              <td class="rank-col">{{ rank }}</td>
              <td v-for="h in conceptHistory" :key="h.trade_date + rank" class="data-col">
                <template v-if="h.rankings[rank - 1]">
                  <div class="cell-name">{{ h.rankings[rank - 1].name || h.rankings[rank - 1].ts_code }}</div>
                  <div class="cell-pct" :class="h.rankings[rank - 1].pct_change >= 0 ? 'up' : 'down'">
                    {{ h.rankings[rank - 1].pct_change >= 0 ? '+' : '' }}{{ h.rankings[rank - 1].pct_change?.toFixed(1) }}%
                  </div>
                  <div class="cell-lead" v-if="h.rankings[rank - 1].lead_stock">
                    {{ h.rankings[rank - 1].lead_stock }}
                  </div>
                </template>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 热点资讯 -->
    <div class="hot-news-zone" v-show="activeTab === 'hot_news'">
      <HotNewsBoard max-height="calc(100vh - 280px)" :show-refresh="true" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import * as echarts from 'echarts'
import { marketApi } from '@/api/modules/market'
import HotNewsBoard from '@/components/dashboard/HotNewsBoard.vue'

// 高对比度配色
const COLORS = {
  strength: '#00BFA5',    // 宝石青
  sentiment: '#263238',   // 深石墨
  barPositive: '#2962FF', // 亮蓝
  barNegative: '#D50000', // 警示红
  up: '#FF1744',          // 纯红
  down: '#00C853',        // 纯绿
  accent: '#FF6D00',      // 橙色强调
  grid: '#1A1A1A',        // 网格线
}

const tabs = [
  { key: 'market_data', label: '走势' },
  { key: 'history_table', label: '数据' },
  { key: 'industry_ranking', label: '行业' },
  { key: 'concept_ranking', label: '板块' },
  { key: 'hot_news', label: '热点' },
]

// 响应式数据
const loading = ref(false)
const latestData = ref<any>(null)
const historyData = ref<any[]>([])
const tableData = ref<any[]>([])
const industryRanking = ref<any[]>([])
const themeRadar = ref<any>(null)
const conceptRanking = ref<any[]>([])
const industryHistory = ref<any[]>([])
const conceptHistory = ref<any[]>([])
const activeTab = ref('market_data')

// 图表引用
const upDownChartRef = ref<HTMLElement | null>(null)
const limitChartRef = ref<HTMLElement | null>(null)
const limitStackChartRef = ref<HTMLElement | null>(null)
const sentimentChartRef = ref<HTMLElement | null>(null)
const trendChartRef = ref<HTMLElement | null>(null)

let upDownChart: echarts.ECharts | null = null
let limitChart: echarts.ECharts | null = null
let limitStackChart: echarts.ECharts | null = null
let sentimentChart: echarts.ECharts | null = null
let trendChart: echarts.ECharts | null = null

// 周期
const cycleDisplayName = computed(() => {
  const map: Record<string, string> = {
    'ice_point': '冰点期', 'decline': '退潮期', 'chaos': '混沌期',
    'incubation': '萌芽期', 'main_upward': '主升期', 'rotation': '分歧期',
  }
  return map[latestData.value?.cycle] || '未知'
})

const cycleClass = computed(() => latestData.value?.cycle?.replace('_', '-') || '')

const getCycleShortName = (cycle: string) => {
  const map: Record<string, string> = {
    'ice_point': '冰点', 'decline': '退潮', 'chaos': '混沌',
    'incubation': '萌芽', 'main_upward': '主升', 'rotation': '分歧',
  }
  return map[cycle] || '-'
}

// 格式化
const formatDate = (d: string) => d ? `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}` : ''
const formatTableDate = (d: string) => d ? `${d.slice(4, 6)}/${d.slice(6, 8)}` : ''
const formatRankingDate = (d: string) => d ? `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}` : ''

const formatAmount = (amount: number | null) => {
  if (!amount) return '-'
  // amount 单位是千元，转换为亿元: 千元 * 1000 / 1e8 = 千元 / 1e5
  const yi = amount / 1e5
  // 超过 1000 亿（0.1万亿）时显示万亿
  if (yi >= 1000) {
    return `${(yi / 10000).toFixed(2)}万亿`
  }
  return `${yi.toFixed(0)}亿`
}

const formatNorth = (money: number | null) => {
  if (money === null || money === undefined) return '-'
  const yi = money / 100
  return `${yi >= 0 ? '+' : ''}${yi.toFixed(1)}亿`
}

// 数据加载
const loadData = async () => {
  loading.value = true
  try {
    const [latestRes, historyRes, tableRes, industryRes, conceptRes, radarRes] = await Promise.all([
      marketApi.getLatest(),
      marketApi.getHistory(30),
      marketApi.getStatsTable(20),
      marketApi.getSectorRanking('industry_top', 30),
      marketApi.getSectorRanking('concept_top', 30),
      marketApi.getThemeRadar(),
    ])
    latestData.value = latestRes
    industryRanking.value = industryRes.rankings || []
    conceptRanking.value = conceptRes.rankings || []
    industryHistory.value = industryRes.history || []
    conceptHistory.value = conceptRes.history || []
    historyData.value = historyRes.history || (historyRes as any).list || []
    tableData.value = tableRes.data || (tableRes as any).list || []
    themeRadar.value = radarRes
    updateCharts()
  } catch (error) {
    console.error('Failed to load market data:', error)
  } finally {
    loading.value = false
  }
}

const refreshData = () => loadData()

// 图表初始化
const initCharts = () => {
  if (upDownChartRef.value) upDownChart = echarts.init(upDownChartRef.value)
  if (limitChartRef.value) limitChart = echarts.init(limitChartRef.value)
  if (limitStackChartRef.value) limitStackChart = echarts.init(limitStackChartRef.value)
  if (sentimentChartRef.value) sentimentChart = echarts.init(sentimentChartRef.value)
  if (trendChartRef.value) trendChart = echarts.init(trendChartRef.value)
}

// 图表配置
const updateCharts = () => {
  if (!historyData.value.length) return
  
  const dates = historyData.value.map(d => d.trade_date?.slice(4, 8) || '')
  
  // 暗色模式适配的图表样式
  const isDark = document.documentElement.classList.contains('dark')
  const axisLineStyle = { lineStyle: { color: isDark ? 'rgba(255,255,255,0.1)' : '#333' } }
  const axisLabelStyle = { color: isDark ? '#64748B' : '#666', fontSize: 11 }
  const gridLineColor = isDark ? 'rgba(255,255,255,0.03)' : '#f0f0f0'
  const gridLineStyle = { lineStyle: { color: gridLineColor, type: 'dashed' as const } }
  
  // 主图: 市场强度与情绪评分
  if (sentimentChart) {
    const strengthData = historyData.value.map(d => d.strength_score)
    const sentimentData = historyData.value.map(d => d.sentiment_score_ema ?? d.sentiment_score)
    const diffData = historyData.value.map(d => {
      const s = d.sentiment_score_ema ?? d.sentiment_score
      return d.strength_diff ?? (d.strength_score - s)
    })
    
    sentimentChart.setOption({
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#1a1a1a',
        borderColor: '#333',
        textStyle: { color: '#fff', fontSize: 12 },
        formatter: (params: any) => {
          const idx = params[0]?.dataIndex
          const d = historyData.value[idx]
          if (!d) return ''
          const diff = d.strength_diff ?? (d.strength_score - (d.sentiment_score_ema ?? d.sentiment_score))
          return `
            <div style="font-weight:600;margin-bottom:6px;">${d.trade_date}</div>
            <div style="color:${COLORS.strength}">● 强度: ${d.strength_score?.toFixed(0)}</div>
            <div style="color:#78909C">● 情绪: ${(d.sentiment_score_ema ?? d.sentiment_score)?.toFixed(0)}</div>
            <div style="color:${diff >= 0 ? COLORS.barPositive : COLORS.barNegative}">差值: ${diff >= 0 ? '+' : ''}${diff?.toFixed(1)}</div>
            <div style="color:#888">量比: ${d.v_ratio?.toFixed(2) ?? '-'}x</div>
          `
        }
      },
      grid: { left: '5%', right: '3%', bottom: 50, top: 30, containLabel: true },
      xAxis: {
        type: 'category',
        data: dates,
        axisLine: axisLineStyle,
        axisLabel: { ...axisLabelStyle, rotate: 45 },
        splitLine: { show: false }
      },
      yAxis: [
        {
          type: 'value',
          min: 0,
          max: 100,
          interval: 20,
          axisLine: { show: false },
          axisLabel: axisLabelStyle,
          splitLine: {
            show: true,
            lineStyle: { color: gridLineColor, width: 1, type: 'dashed' }
          }
        },
        {
          type: 'value',
          min: -50,
          max: 50,
          axisLine: { show: false },
          axisLabel: { show: false },
          splitLine: { show: false }
        }
      ],
      series: [
        // 强弱差柱图 - 加宽加粗
        {
          name: '强弱差',
          type: 'bar',
          yAxisIndex: 1,
          barWidth: '70%',
          itemStyle: {
            color: (params: any) => params.value >= 0 ? COLORS.barPositive : COLORS.barNegative
          },
          data: diffData
        },
        // 市场强度线 - 宝石青 3px + 数值标签
        {
          name: '市场强度',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: { color: COLORS.strength, width: 3 },
          itemStyle: { 
            color: COLORS.strength,
            borderColor: '#fff',
            borderWidth: 2
          },
          label: {
            show: true,
            position: 'top',
            fontSize: 10,
            fontWeight: 'bold',
            color: COLORS.strength,
            formatter: (p: any) => p.value?.toFixed(0)
          },
          data: strengthData,
          markLine: {
            silent: true,
            symbol: 'none',
            lineStyle: { color: '#BDBDBD', width: 1, type: 'dashed' },
            data: [
              { yAxis: 20, label: { show: false } },
              { yAxis: 50, label: { formatter: '强弱线', position: 'end', color: '#999', fontSize: 10 } },
              { yAxis: 80, label: { show: false } }
            ]
          }
        },
        // 情绪评分线 - 深石墨 2px + 数值标签
        {
          name: '情绪评分',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 5,
          lineStyle: { color: COLORS.sentiment, width: 2 },
          itemStyle: { color: COLORS.sentiment },
          label: {
            show: true,
            position: 'bottom',
            fontSize: 9,
            color: COLORS.sentiment,
            formatter: (p: any) => p.value?.toFixed(0)
          },
          data: sentimentData
        }
      ]
    })
  }
  
  // 涨跌停趋势图
  if (limitChart) {
    limitChart.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: '5%', right: '4%', bottom: 40, top: 35, containLabel: true },
      xAxis: { type: 'category', data: dates, axisLine: axisLineStyle, axisLabel: { ...axisLabelStyle, rotate: 45 } },
      yAxis: [
        { type: 'value', axisLine: { show: false }, axisLabel: axisLabelStyle, splitLine: gridLineStyle },
        { type: 'value', max: 15, axisLine: { show: false }, axisLabel: { show: false }, splitLine: { show: false } }
      ],
      series: [
        {
          name: '涨停',
          type: 'bar',
          barWidth: '30%',
          itemStyle: { color: COLORS.up },
          label: {
            show: true,
            position: 'top',
            fontSize: 9,
            color: COLORS.up,
            formatter: (p: any) => p.value > 0 ? p.value : ''
          },
          data: historyData.value.map(d => d.limit_up_count)
        },
        {
          name: '跌停',
          type: 'bar',
          barWidth: '30%',
          itemStyle: { color: COLORS.down },
          label: {
            show: true,
            position: 'top',
            fontSize: 9,
            color: COLORS.down,
            formatter: (p: any) => p.value > 0 ? p.value : ''
          },
          data: historyData.value.map(d => d.limit_down_count)
        },
        {
          name: '高度',
          type: 'line',
          yAxisIndex: 1,
          symbol: 'circle',
          symbolSize: 8,
          lineStyle: { color: COLORS.accent, width: 2.5 },
          itemStyle: { color: COLORS.accent, borderColor: '#fff', borderWidth: 2 },
          label: { show: true, position: 'top', fontSize: 11, fontWeight: 'bold', color: COLORS.accent },
          data: historyData.value.map(d => d.max_limit_height)
        }
      ]
    })
  }
  
  // 涨跌停堆叠对比图
  if (limitStackChart) {
    // 计算百分比用于堆叠高度
    const limitUpRatioData = historyData.value.map(d => {
      const total = (d.limit_up_count || 0) + (d.limit_down_count || 0)
      return total > 0 ? ((d.limit_up_count || 0) / total * 100) : 0
    })
    const limitDownRatioData = historyData.value.map(d => {
      const total = (d.limit_up_count || 0) + (d.limit_down_count || 0)
      return total > 0 ? ((d.limit_down_count || 0) / total * 100) : 0
    })
    
    // 保存原始数值用于标签显示
    const limitUpCountData = historyData.value.map(d => d.limit_up_count || 0)
    const limitDownCountData = historyData.value.map(d => d.limit_down_count || 0)
    
    limitStackChart.setOption({
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const idx = params[0]?.dataIndex
          const d = historyData.value[idx]
          if (!d) return ''
          const total = (d.limit_up_count || 0) + (d.limit_down_count || 0)
          const upRatio = total > 0 ? (d.limit_up_count / total * 100).toFixed(1) : 0
          const downRatio = total > 0 ? (d.limit_down_count / total * 100).toFixed(1) : 0
          return `
            <div style="font-weight:600;margin-bottom:6px;">${d.trade_date?.slice(4, 8)}</div>
            <div style="color:${COLORS.up}">● 涨停: ${d.limit_up_count} (${upRatio}%)</div>
            <div style="color:${COLORS.down}">● 跌停: ${d.limit_down_count} (${downRatio}%)</div>
          `
        }
      },
      grid: { left: '4%', right: '3%', bottom: 50, top: 30, containLabel: true },
      xAxis: { type: 'category', data: dates, axisLine: axisLineStyle, axisLabel: { ...axisLabelStyle, rotate: 45 } },
      yAxis: { 
        type: 'value', 
        max: 100,
        axisLine: { show: false }, 
        axisLabel: { ...axisLabelStyle, formatter: '{value}%' }, 
        splitLine: gridLineStyle 
      },
      series: [
        {
          name: '涨停',
          type: 'bar',
          stack: 'total',
          barWidth: '60%',
          itemStyle: { color: COLORS.up },
          label: {
            show: true,
            position: 'inside',
            fontSize: 11,
            color: '#fff',
            fontWeight: 'bold',
            formatter: (p: any) => limitUpCountData[p.dataIndex] > 0 ? limitUpCountData[p.dataIndex] : ''
          },
          data: limitUpRatioData
        },
        {
          name: '跌停',
          type: 'bar',
          stack: 'total',
          barWidth: '60%',
          itemStyle: { color: COLORS.down },
          label: {
            show: true,
            position: 'inside',
            fontSize: 11,
            color: '#fff',
            fontWeight: 'bold',
            formatter: (p: any) => limitDownCountData[p.dataIndex] > 0 ? limitDownCountData[p.dataIndex] : ''
          },
          data: limitDownRatioData
        }
      ]
    })
  }
  
  // 涨跌家数堆叠柱状图（显示具体数值）
  if (upDownChart) {
    // 计算百分比用于堆叠高度
    const upRatioData = historyData.value.map(d => {
      const total = (d.up_count || 0) + (d.down_count || 0)
      return total > 0 ? ((d.up_count || 0) / total * 100) : 0
    })
    const downRatioData = historyData.value.map(d => {
      const total = (d.up_count || 0) + (d.down_count || 0)
      return total > 0 ? ((d.down_count || 0) / total * 100) : 0
    })
    
    // 保存原始数值用于标签显示
    const upCountData = historyData.value.map(d => d.up_count || 0)
    const downCountData = historyData.value.map(d => d.down_count || 0)
    
    upDownChart.setOption({
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const idx = params[0]?.dataIndex
          const d = historyData.value[idx]
          if (!d) return ''
          const total = (d.up_count || 0) + (d.down_count || 0)
          const upRatio = total > 0 ? (d.up_count / total * 100).toFixed(1) : 0
          const downRatio = total > 0 ? (d.down_count / total * 100).toFixed(1) : 0
          return `
            <div style="font-weight:600;margin-bottom:6px;">${d.trade_date?.slice(4, 8)}</div>
            <div style="color:${COLORS.up}">● 上涨: ${d.up_count} (${upRatio}%)</div>
            <div style="color:${COLORS.down}">● 下跌: ${d.down_count} (${downRatio}%)</div>
          `
        }
      },
      grid: { left: '4%', right: '3%', bottom: 50, top: 30, containLabel: true },
      xAxis: { type: 'category', data: dates, axisLine: axisLineStyle, axisLabel: { ...axisLabelStyle, rotate: 45 } },
      yAxis: { 
        type: 'value', 
        max: 100,
        axisLine: { show: false }, 
        axisLabel: { ...axisLabelStyle, formatter: '{value}%' }, 
        splitLine: gridLineStyle 
      },
      series: [
        {
          name: '上涨',
          type: 'bar',
          stack: 'total',
          barWidth: '60%',
          itemStyle: { color: COLORS.up },
          label: {
            show: true,
            position: 'inside',
            fontSize: 11,
            color: '#fff',
            fontWeight: 'bold',
            formatter: (p: any) => upCountData[p.dataIndex]
          },
          data: upRatioData
        },
        {
          name: '下跌',
          type: 'bar',
          stack: 'total',
          barWidth: '60%',
          itemStyle: { color: COLORS.down },
          label: {
            show: true,
            position: 'inside',
            fontSize: 11,
            color: '#fff',
            fontWeight: 'bold',
            formatter: (p: any) => downCountData[p.dataIndex]
          },
          data: downRatioData
        }
      ]
    })
  }
  
  // 综合趋势图
  if (trendChart) {
    // 原始数据准备
    const volumeRaw = historyData.value.map(d => (d.total_amount || 0) / 1e5) // 转换为亿元
    const limitUpRaw = historyData.value.map(d => d.limit_up_count || 0)
    const limitDownRaw = historyData.value.map(d => d.limit_down_count || 0)
    const heightRaw = historyData.value.map(d => d.max_limit_height || 0)
    const upCountRaw = historyData.value.map(d => d.up_count || 0)
    const downCountRaw = historyData.value.map(d => d.down_count || 0)
    const sentimentRaw = historyData.value.map(d => d.sentiment_score_ema ?? d.sentiment_score ?? 0)
    const strengthRaw = historyData.value.map(d => d.strength_score || 0)
    
    // 归一化函数：将数据映射到 0-100 范围
    const normalize = (arr: number[]) => {
      const min = Math.min(...arr)
      const max = Math.max(...arr)
      const range = max - min || 1
      return arr.map(v => ((v - min) / range) * 100)
    }
    
    // 归一化数据用于显示
    const volumeNorm = normalize(volumeRaw)
    const limitUpNorm = normalize(limitUpRaw)
    const limitDownNorm = normalize(limitDownRaw)
    const heightNorm = normalize(heightRaw)
    const upCountNorm = normalize(upCountRaw)
    const downCountNorm = normalize(downCountRaw)
    // 评分本身就是 0-100，不需要归一化
    const sentimentNorm = sentimentRaw
    const strengthNorm = strengthRaw
    
    trendChart.setOption({
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#1e293b',
        borderColor: '#334155',
        textStyle: { color: '#f1f5f9', fontSize: 12 },
        formatter: (params: any) => {
          const idx = params[0]?.dataIndex
          const d = historyData.value[idx]
          if (!d) return ''
          return `
            <div style="font-weight:600;margin-bottom:8px;border-bottom:1px solid #475569;padding-bottom:6px;">${d.trade_date}</div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 16px;font-size:12px;">
              <div style="color:#60a5fa">● 成交量: ${volumeRaw[idx].toFixed(0)}亿</div>
              <div style="color:#f87171">● 涨停: ${limitUpRaw[idx]}</div>
              <div style="color:#4ade80">● 跌停: ${limitDownRaw[idx]}</div>
              <div style="color:#fbbf24">● 天梯: ${heightRaw[idx]}板</div>
              <div style="color:#f472b6">● 上涨: ${upCountRaw[idx]}</div>
              <div style="color:#2dd4bf">● 下跌: ${downCountRaw[idx]}</div>
              <div style="color:#a78bfa">● 情绪: ${sentimentRaw[idx]?.toFixed(0)}</div>
              <div style="color:#34d399">● 强度: ${strengthRaw[idx]?.toFixed(0)}</div>
            </div>
          `
        }
      },
      legend: {
        data: ['成交量', '涨停', '跌停', '天梯高度', '上涨家数', '下跌家数', '情绪评分', '市场强度'],
        top: 0,
        textStyle: { color: '#64748b', fontSize: 11 },
        itemWidth: 16,
        itemHeight: 8,
        itemGap: 12,
        selected: {
          '成交量': true,
          '涨停': true,
          '跌停': true,
          '天梯高度': true,
          '上涨家数': false,
          '下跌家数': false,
          '情绪评分': true,
          '市场强度': true
        }
      },
      grid: { left: '3%', right: '3%', bottom: 50, top: 40, containLabel: true },
      xAxis: {
        type: 'category',
        data: dates,
        axisLine: axisLineStyle,
        axisLabel: { ...axisLabelStyle, rotate: 45 }
      },
      yAxis: {
        type: 'value',
        name: '相对强度 (%)',
        min: 0,
        max: 100,
        axisLine: { show: false },
        axisLabel: { ...axisLabelStyle, formatter: '{value}' },
        splitLine: gridLineStyle
      },
      series: [
        {
          name: '成交量',
          type: 'bar',
          barWidth: '40%',
          itemStyle: { color: 'rgba(96, 165, 250, 0.25)' },
          data: volumeNorm,
          z: 0
        },
        {
          name: '涨停',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 5,
          lineStyle: { color: '#f87171', width: 2 },
          itemStyle: { color: '#f87171' },
          data: limitUpNorm
        },
        {
          name: '跌停',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 5,
          lineStyle: { color: '#4ade80', width: 2 },
          itemStyle: { color: '#4ade80' },
          data: limitDownNorm
        },
        {
          name: '天梯高度',
          type: 'line',
          smooth: false,
          symbol: 'diamond',
          symbolSize: 7,
          lineStyle: { color: '#fbbf24', width: 2, type: 'dashed' },
          itemStyle: { color: '#fbbf24' },
          data: heightNorm
        },
        {
          name: '上涨家数',
          type: 'line',
          smooth: true,
          symbol: 'none',
          lineStyle: { color: '#f472b6', width: 1.5 },
          data: upCountNorm
        },
        {
          name: '下跌家数',
          type: 'line',
          smooth: true,
          symbol: 'none',
          lineStyle: { color: '#2dd4bf', width: 1.5 },
          data: downCountNorm
        },
        {
          name: '情绪评分',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 4,
          lineStyle: { color: '#a78bfa', width: 2 },
          itemStyle: { color: '#a78bfa' },
          data: sentimentNorm
        },
        {
          name: '市场强度',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 4,
          lineStyle: { color: '#34d399', width: 2 },
          itemStyle: { color: '#34d399' },
          data: strengthNorm
        }
      ]
    })
  }
}

const handleResize = () => {
  upDownChart?.resize()
  limitChart?.resize()
  trendChart?.resize()
  limitStackChart?.resize()
  sentimentChart?.resize()
}

onMounted(() => {
  initCharts()
  loadData()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  upDownChart?.dispose()
  limitChart?.dispose()
  limitStackChart?.dispose()
  sentimentChart?.dispose()
  trendChart?.dispose()
})

watch(activeTab, () => setTimeout(handleResize, 100))
</script>

<style scoped>
/* 全局容器 - 自适应屏幕 */
.market-terminal {
  width: 100%;
  min-height: 100vh;
  margin: 0;
  padding: 0;
  background: var(--bg-base);
  display: grid;
  grid-template-rows: auto auto 1fr;
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', Roboto, sans-serif;
  box-sizing: border-box;
  overflow-x: hidden;
  transition: background-color var(--transition-normal);
}

/* 顶部统计条 */
.stats-strip {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px 0;
  padding: 12px 16px;
  min-height: 72px;
  background: var(--bg-muted);
  border-bottom: 2px solid var(--border-default);
  width: 100%;
  box-sizing: border-box;
  overflow-x: auto;
}

/* 主线雷达样式 */
.theme-radar-strip {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 10px 24px;
  background: linear-gradient(90deg, #1a1a2e 0%, #16213e 100%);
  border-bottom: 1px solid #0f3460;
  width: 100%;
  box-sizing: border-box;
}

.radar-title {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #00BFA5;
  font-weight: 600;
  font-size: 14px;
  white-space: nowrap;
}

.radar-icon {
  animation: pulse 2s infinite;
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}

.radar-items {
  display: flex;
  gap: 12px;
  flex: 1;
}

.radar-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px;
  background: rgba(255,255,255,0.08);
  border-radius: 20px;
  border: 1px solid rgba(255,255,255,0.1);
  transition: all 0.2s;
}

.radar-item:hover {
  background: rgba(255,255,255,0.15);
  transform: translateY(-1px);
}

.radar-item.main_theme {
  background: rgba(0, 191, 165, 0.2);
  border-color: #00BFA5;
}

.radar-item.strong_focus {
  background: rgba(255, 143, 0, 0.15);
  border-color: #FF8F00;
}

.radar-name {
  color: #fff;
  font-weight: 600;
  font-size: 13px;
}

.radar-days {
  color: #9E9E9E;
  font-size: 11px;
}

.radar-trend {
  font-weight: 700;
  font-size: 13px;
}

.radar-trend.up {
  color: #FF1744;
}

.radar-trend.down {
  color: #00C853;
}

.radar-trend.flat {
  color: #9E9E9E;
}

.rotation-hint {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-left: auto;
  padding-left: 16px;
  border-left: 1px solid rgba(255,255,255,0.1);
}

.rotation-label {
  color: #757575;
  font-size: 12px;
}

.rotation-item {
  font-size: 12px;
  padding: 3px 8px;
  border-radius: 4px;
  background: rgba(255,255,255,0.05);
}

.rotation-item.rising {
  color: #FF1744;
}

.rotation-item.falling {
  color: #00C853;
}

.stat-block {
  display: flex;
  flex-direction: column;
  padding: 8px 12px;
  justify-content: center;
  border-right: 1px solid var(--border-default);
  flex: 0 1 auto;
  min-width: 60px;
}

.stat-block:last-of-type {
  border-right: none;
}

.stat-label {
  font-size: 11px;
  font-weight: 600;
  color: var(--text-tertiary);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 4px;
  white-space: nowrap;
}

.stat-value {
  font-size: 20px;
  font-weight: 700;
  color: var(--text-primary);
  font-variant-numeric: tabular-nums;
  line-height: 1;
  white-space: nowrap;
}

.stat-block.primary .stat-value { color: #00BFA5; }
.stat-block.up .stat-value { color: #FF1744; }
.stat-block.down .stat-value { color: #00C853; }
.stat-block.accent .stat-value { color: #FF6D00; }

.stat-divider {
  width: 2px;
  height: 32px;
  background: var(--border-muted);
  margin: 0 8px;
  flex: none;
}

/* 周期指示器 */
.cycle-indicator {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 14px;
  margin-left: auto;
  background: var(--bg-subtle);
  border-radius: 6px;
  flex: none;
}

.cycle-pulse {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background: var(--text-tertiary);
  animation: pulse 2s infinite;
}

@keyframes pulse {
  0%, 100% { opacity: 1; transform: scale(1); }
  50% { opacity: 0.6; transform: scale(1.2); }
}

.cycle-indicator.ice-point .cycle-pulse { background: #2962FF; }
.cycle-indicator.decline .cycle-pulse { background: #D50000; }
.cycle-indicator.chaos .cycle-pulse { background: #AA00FF; }
.cycle-indicator.incubation .cycle-pulse { background: #00C853; }
.cycle-indicator.main-upward .cycle-pulse { background: #FFAB00; }
.cycle-indicator.rotation .cycle-pulse { background: #00B8D4; }

.cycle-name {
  font-size: 14px;
  font-weight: 600;
  color: var(--text-primary);
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-left: 12px;
  flex: none;
}

.trade-date {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-secondary);
}

.refresh-btn {
  width: 36px;
  height: 36px;
  border: none;
  border-radius: 6px;
  background: var(--bg-muted);
  color: var(--text-primary);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background 0.2s;
}

.refresh-btn:hover { background: var(--bg-subtle); }
.refresh-btn:disabled { opacity: 0.5; cursor: not-allowed; }
.refresh-btn svg { width: 18px; height: 18px; }
.refresh-btn svg.spinning { animation: spin 1s linear infinite; }

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

/* 导航栏 */
.nav-bar {
  display: flex;
  gap: 0;
  padding: 0 16px;
  background: var(--bg-elevated);
  border-bottom: 1px solid var(--border-default);
  width: 100%;
  box-sizing: border-box;
}

.nav-tab {
  padding: 14px 24px;
  font-size: 13px;
  font-weight: 600;
  color: var(--text-tertiary);
  background: none;
  border: none;
  border-bottom: 3px solid transparent;
  cursor: pointer;
  transition: all 0.2s;
}

.nav-tab:hover { color: var(--text-primary); }
.nav-tab.active {
  color: var(--primary-500);
  border-bottom-color: var(--primary-500);
}

/* 图表区域 */
.chart-zone {
  display: flex;
  flex-direction: column;
  gap: 0;
  padding: 16px;
  background: var(--bg-base);
  width: 100%;
  box-sizing: border-box;
  overflow-x: hidden;
  overflow-y: auto;
}

.chart-container {
  background: var(--bg-elevated);
  border: 1px solid var(--border-default);
  border-radius: 12px;
  width: 100%;
  box-sizing: border-box;
  overflow: hidden;
}

.chart-container.main-chart {
  min-height: 360px;
}

.chart-title {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 20px;
  border-bottom: 1px solid var(--border-light);
}

.chart-title span {
  font-size: 14px;
  font-weight: 700;
  color: var(--text-primary);
}

.chart-legend {
  display: flex;
  gap: 20px;
}

.legend {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  color: var(--text-tertiary);
}

.legend i {
  width: 14px;
  height: 4px;
  border-radius: 2px;
}

.legend.strength i { background: #00BFA5; height: 4px; }
.legend.sentiment i { background: #263238; height: 3px; }
.legend.bar-pos i { background: #2962FF; height: 10px; width: 10px; border-radius: 2px; }
.legend.bar-neg i { background: #D50000; height: 10px; width: 10px; border-radius: 2px; }

.chart-canvas {
  width: 100%;
  height: 280px;
  min-width: 0;
}

.chart-canvas.main {
  height: 340px;
}

.chart-canvas.sub {
  height: 300px;
}

.chart-container.sub-chart {
  margin-top: 16px;
}

/* 表格区域 - 自适应 */
.table-zone {
  width: 100%;
  padding: 16px;
  background: var(--bg-base);
  box-sizing: border-box;
  overflow-x: auto;
}

.table-wrapper {
  width: 100%;
  background: var(--bg-elevated);
  border: 1px solid var(--border-default);
  border-radius: 12px;
  overflow-x: auto;
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.data-table thead {
  position: sticky;
  top: 0;
  z-index: 10;
}

.data-table th {
  padding: 14px 16px;
  background: var(--bg-muted);
  color: var(--text-primary);
  font-weight: 600;
  text-align: center;
  white-space: nowrap;
  border: none;
}

.data-table th.fixed-col {
  position: sticky;
  left: 0;
  z-index: 11;
}

.data-table td {
  padding: 12px 16px;
  text-align: center;
  border-bottom: 1px solid var(--border-light);
  font-variant-numeric: tabular-nums;
  color: var(--text-secondary);
}

.data-table td.fixed-col {
  position: sticky;
  left: 0;
  background: inherit;
  z-index: 5;
}

.data-table tbody tr:nth-child(odd) { background: var(--bg-elevated); }
.data-table tbody tr:nth-child(even) { background: var(--bg-muted); }
.data-table tbody tr:hover { background: var(--bg-active) !important; }

.date-cell { font-weight: 600; color: var(--text-primary); }
.up-cell { color: var(--stock-up); }
.down-cell { color: var(--stock-down); }
.up-cell.bold, .down-cell.bold { font-weight: 700; }
.height-cell { color: #FF6D00; font-weight: 700; font-size: 15px; }
.score-cell { color: var(--primary-500); font-weight: 600; }

.cycle-tag {
  display: inline-block;
  padding: 3px 10px;
  border-radius: 6px;
  font-size: 11px;
  font-weight: 600;
  background: var(--bg-muted);
  color: var(--text-secondary);
}

.cycle-tag.ice-point { background: #BBDEFB; color: #0D47A1; }
.cycle-tag.decline { background: #FFCDD2; color: #B71C1C; }
.cycle-tag.chaos { background: #E1BEE7; color: #4A148C; }
.cycle-tag.incubation { background: #C8E6C9; color: #1B5E20; }
.cycle-tag.main-upward { background: #FFE0B2; color: #E65100; }
.cycle-tag.rotation { background: #B2EBF2; color: #006064; }

/* 响应式 */

@media (max-width: 1400px) {
  .stat-block {
    padding: 6px 10px;
  }
  .stat-value {
    font-size: 18px;
  }
  .stat-label {
    font-size: 10px;
  }
}

@media (max-width: 1200px) {
  .stat-block {
    padding: 6px 8px;
  }
  .stat-value {
    font-size: 16px;
  }
  .stat-divider {
    margin: 0 4px;
    height: 24px;
  }
  .cycle-indicator {
    padding: 6px 12px;
  }
  .cycle-name {
    font-size: 12px;
  }
}

@media (max-width: 1024px) {
  .stats-strip {
    padding: 10px 12px;
  }
  .stat-block {
    padding: 4px 6px;
    min-width: 50px;
  }
  .stat-value {
    font-size: 14px;
  }
  .stat-divider {
    display: none;
  }
  .header-actions {
    margin-left: 8px;
  }
  .trade-date {
    font-size: 12px;
  }
}

@media (max-width: 768px) {
  .stats-strip {
    flex-wrap: wrap;
    height: auto;
    padding: 10px 12px;
    gap: 4px;
  }
  
  .stat-block {
    padding: 4px 8px;
    flex: none;
    min-width: auto;
    border-right: none;
  }
  
  .stat-value {
    font-size: 16px;
  }
  
  .stat-divider { display: none; }
  
  .cycle-indicator {
    margin-left: 0;
    margin-top: 8px;
  }
  
  .header-actions {
    width: 100%;
    justify-content: flex-end;
    margin-top: 8px;
    margin-left: 0;
  }
  
  .chart-zone {
    padding: 8px;
  }
  
  .chart-container.sub-chart {
    margin-top: 8px;
  }
  
  .chart-canvas.sub {
    height: 250px;
  }
}

/* 排行榜时间轴表格 */
.ranking-table-zone {
  width: 100%;
  padding: 16px;
  background: var(--bg-base);
  box-sizing: border-box;
  overflow: hidden;
}

.ranking-table-wrapper {
  width: 100%;
  overflow-x: auto;
  overflow-y: auto;
  background: var(--bg-elevated);
  border: 1px solid var(--border-default);
  border-radius: 12px;
  max-height: calc(100vh - 220px);
  /* 强制显示滚动条 */
  scrollbar-width: auto;
  -ms-overflow-style: auto;
}

/* 确保滚动条可见 - Webkit浏览器 */
.ranking-table-wrapper::-webkit-scrollbar {
  height: 14px;
  width: 14px;
  display: block !important;
}

.ranking-table-wrapper::-webkit-scrollbar-track {
  background: var(--bg-muted);
  border-radius: 7px;
}

.ranking-table-wrapper::-webkit-scrollbar-thumb {
  background: var(--text-muted);
  border-radius: 7px;
  border: 2px solid var(--bg-muted);
}

.ranking-table-wrapper::-webkit-scrollbar-thumb:hover {
  background: var(--text-tertiary);
}

.ranking-table-wrapper::-webkit-scrollbar-corner {
  background: var(--bg-muted);
}

.ranking-timeline-table {
  /* 固定表格总宽度，确保超出容器产生滚动 */
  width: max-content;
  min-width: 2000px;
  border-collapse: collapse;
  font-size: 13px;
}

.ranking-timeline-table thead {
  position: sticky;
  top: 0;
  z-index: 10;
}

.ranking-timeline-table th {
  padding: 14px 12px;
  background: var(--bg-muted);
  color: var(--text-primary);
  font-weight: 600;
  font-size: 14px;
  text-align: center;
  white-space: nowrap;
  border-right: 1px solid var(--border-light);
}

.ranking-timeline-table th:last-child {
  border-right: none;
}

.ranking-timeline-table th.rank-col {
  position: sticky;
  left: 0;
  z-index: 11;
  background: var(--bg-subtle);
  width: 60px;
  min-width: 60px;
}

.ranking-timeline-table th.date-col {
  /* 每列固定宽度，30天 × 180px = 5400px */
  width: 180px;
  min-width: 180px;
}

.ranking-timeline-table td {
  padding: 12px 10px;
  text-align: center;
  border-bottom: 1px solid var(--border-light);
  border-right: 1px solid var(--border-light);
  vertical-align: top;
}

.ranking-timeline-table td:last-child {
  border-right: none;
}

.ranking-timeline-table td.rank-col {
  position: sticky;
  left: 0;
  background: var(--bg-muted);
  font-weight: 700;
  font-size: 16px;
  color: var(--text-primary);
  z-index: 5;
  width: 60px;
  min-width: 60px;
}

.ranking-timeline-table td.data-col {
  /* 与表头对齐 */
  width: 180px;
  min-width: 180px;
  background: var(--bg-elevated);
}

.ranking-timeline-table tbody tr:nth-child(odd) td.data-col {
  background: var(--bg-inset);
}

.ranking-timeline-table tbody tr:hover td {
  background: var(--bg-active) !important;
}

.cell-name {
  font-weight: 600;
  color: var(--text-primary);
  font-size: 14px;
  line-height: 1.4;
  margin-bottom: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 150px;
}

.cell-pct {
  font-size: 14px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  margin-bottom: 3px;
}

.cell-pct.up {
  color: var(--stock-up);
}

.cell-pct.down {
  color: var(--stock-down);
}

.cell-lead {
  font-size: 12px;
  color: var(--text-tertiary);
  margin-top: 2px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 150px;
}

.no-data {
  padding: 40px 20px;
  text-align: center;
  color: var(--text-muted);
  font-size: 14px;
}

/* 热点资讯区域 */
.hot-news-zone {
  padding: 16px;
  background: var(--bg-base);
  width: 100%;
  box-sizing: border-box;
}

/* 全局溢出控制 */
* {
  box-sizing: border-box;
}
</style>
