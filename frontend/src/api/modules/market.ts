/**
 * 市场分析 API
 */

import { api } from '../client'

export interface MarketScores {
  sentiment: number
  strength: number
}

export interface MarketStats {
  up_count: number
  down_count: number
  flat_count: number
  total_stocks: number
  up_ratio: number
  down_ratio: number
  limit_up_count: number
  limit_down_count: number
  broken_limit_count: number
  max_limit_height: number
  limit_1: number
  limit_2: number
  limit_3: number
  limit_4: number
  limit_5: number
  limit_6_plus: number
  total_limit_up: number
  total_amount: number  // 亿元
  sh_amount: number
  sz_amount: number
  north_money: number   // 亿元
  hgt?: number
  sgt?: number
}

export interface MarketLatest {
  trade_date: string
  scores: MarketScores
  cycle: string
  cycle_name: string
  cycle_reason: string
  stats: MarketStats
}

export interface MarketHistoryItem {
  trade_date: string
  sentiment_score: number
  strength_score: number
  cycle: string
  up_count: number
  down_count: number
  up_ratio: number
  limit_up_count: number
  limit_down_count: number
  broken_limit_count: number
  max_limit_height: number
  limit_1: number
  limit_2: number
  limit_3: number
  limit_4: number
  limit_5: number
  limit_6_plus: number
  total_amount: number
  north_money: number
}

export interface SectorRanking {
  rank: number
  ts_code: string
  name: string
  pct_change: number
  net_amount: number
  lead_stock?: string  // 领涨个股
}

export interface StatsTableItem {
  trade_date: string
  up_count: number
  down_count: number
  limit_up_count: number
  limit_down_count: number
  limit_1: number
  limit_2: number
  limit_3: number
  limit_4: number
  limit_5: number
  limit_6_plus: number
  max_limit_height: number
}

// 主线雷达数据
export interface ThemeRadarItem {
  name: string
  rank_change: number
  trend: 'up' | 'down' | 'flat'
  consecutive_days: number
  status: string
}

export interface RotationItem {
  name: string
  latest_rank: number | null
  prev_rank: number | null
  change: number
  trend: 'rising' | 'falling'
}

export interface ThemeRadar {
  trade_date: string
  radar: ThemeRadarItem[]
  rotation: RotationItem[]
}

// 主线分析数据
export interface MainTheme {
  name: string
  ts_code?: string
  status: string
  consecutive_days: number
  avg_rank: number
  high_board_count?: number
  rank_change: number
  reason?: string
}

export interface ThemeAnalysis {
  trade_date: string
  main_themes: MainTheme[]
  strong_focus: MainTheme[]
  rotation_analysis: RotationItem[]
}

// 板块时间线数据
export interface SectorTimelineItem {
  name: string
  ts_code?: string
  appearances: number
  avg_rank: number
  ranks: Record<string, { rank: number; pct_change: number; lead_stock: string }>
}

export interface SectorTimeline {
  dates: string[]
  sectors: SectorTimelineItem[]
}

// 板块评分数据
export interface SectorScore {
  name: string
  ts_code?: string
  score: number
  frequency: number
  short_strength: number
  avg_rank: number
  today_pct: number
  high_board_count: number
  max_board: number
  net_amount: number
  lead_stock: string
  category: 'CORE' | 'PULSE' | 'FADE' | 'ACTIVE' | '-'
  status: 'core' | 'pulse' | 'fading' | 'active' | 'normal'
  reason: string
  rank_trend: number[]
}

export interface SectorScoresResult {
  trade_date: string
  lookback_days: number
  total_sectors: number
  sectors: SectorScore[]
  core_themes: SectorScore[]
  pulse_themes: SectorScore[]
  fading_themes: SectorScore[]
  diagnosis: string
}

export interface ScatterPoint {
  name: string
  x: number
  y: number
  score: number
  category: string
  status: string
  reason: string
}

export interface ScatterZone {
  name: string
  xMin: number
  xMax: number
  yMin: number
  yMax: number
  color: string
}

export interface SectorScatterResult {
  trade_date: string
  scatter: ScatterPoint[]
  zones: ScatterZone[]
}

export const marketApi = {
  /**
   * 获取最新市场数据
   */
  getLatest: () => 
    api.get<MarketLatest>('/market/latest'),

  /**
   * 获取历史数据
   */
  getHistory: (days: number = 30) => 
    api.get<{ history: MarketHistoryItem[] }>('/market/history', { params: { days } }),

  /**
   * 获取板块排名
   */
  getSectorRanking: (rankingType: string = 'industry_top', days: number = 1, tradeDate?: string) =>
    api.get<{ trade_date: string; rankings: SectorRanking[]; history: any[] }>('/market/sector-ranking', {
      params: { ranking_type: rankingType, days, trade_date: tradeDate }
    }),

  /**
   * 获取统计表格数据
   */
  getStatsTable: (days: number = 15) =>
    api.get<{ data: StatsTableItem[] }>('/market/stats-table', { params: { days } }),

  /**
   * 获取主线雷达数据
   */
  getThemeRadar: (tradeDate?: string) =>
    api.get<ThemeRadar>('/market/theme-radar', { params: { trade_date: tradeDate } }),

  /**
   * 获取完整主线分析
   */
  getThemeAnalysis: (tradeDate?: string, days: number = 5) =>
    api.get<ThemeAnalysis>('/market/theme-analysis', { params: { trade_date: tradeDate, days } }),

  /**
   * 获取板块时间线数据
   */
  getSectorTimeline: (rankingType: string = 'industry_top', days: number = 10) =>
    api.get<SectorTimeline>('/market/sector-timeline', { params: { ranking_type: rankingType, days } }),

  /**
   * 获取板块长周期评分
   */
  getSectorScores: (tradeDate?: string, days: number = 20) =>
    api.get<SectorScoresResult>('/market/sector-scores', { params: { trade_date: tradeDate, days } }),

  /**
   * 获取板块散点图数据
   */
  getSectorScatter: (tradeDate?: string) =>
    api.get<SectorScatterResult>('/market/sector-scatter', { params: { trade_date: tradeDate } }),

  // ==================== 热点新闻 ====================

  /**
   * 获取热点新闻
   */
  getHotNews: (source?: string, limit: number = 50) =>
    api.get<HotNewsResult>('/market/hot_news', { params: { source, limit } }),

  /**
   * 获取热点新闻来源列表
   */
  getHotNewsSources: () =>
    api.get<{ sources: HotNewsSource[] }>('/market/hot_news/sources'),

  /**
   * 刷新热点新闻
   */
  refreshHotNews: (source?: string) =>
    api.post<{ 
      success_count?: number
      fail_count?: number
      total_news?: number
      count?: number
      message?: string 
    }>(
      '/market/hot_news/refresh',
      null,
      { params: { source } }
    ),

  /**
   * 获取热点新闻统计
   */
  getHotNewsStats: () =>
    api.get<HotNewsStats>('/market/hot_news/stats'),
}

// 热点新闻类型
export interface HotNewsItem {
  source: string
  source_name: string
  title: string
  url: string
  hot: number
  rank: number
  extra: Record<string, any>
  updated_at: string
}

export interface HotNewsSource {
  id: string
  name: string
}

export interface HotNewsResult {
  news: HotNewsItem[]
  total: number
  updated_at?: string
}

export interface HotNewsStats {
  stats: {
    source: string
    source_name: string
    count: number
    updated_at: string
  }[]
  total: number
}

export default marketApi
