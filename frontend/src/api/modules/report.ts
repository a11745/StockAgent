/**
 * 报告回顾 API
 */

import { api } from '../client'

export interface ReportStats {
  event_count: number
  news_count: number
  high_importance_count: number
  macro_count: number
  industry_count: number
  stock_count: number
  hot_count: number
  time_range_start?: string
  time_range_end?: string
}

export interface ReportListItem {
  id: string
  type: 'morning' | 'noon'
  date: string
  title: string
  overview: string
  stats: ReportStats
  created_at: string
  pushed: {
    wechat: boolean
    websocket: boolean
  }
}

export interface ReportListResponse {
  items: ReportListItem[]
  total: number
  page: number
  page_size: number
  total_pages: number
}

export interface ReportSectionItem {
  event_id: string
  title: string
  summary: string
  importance: 'high' | 'medium' | 'low'
  news_count: number
  ts_codes: string[]
  event_time?: string
}

export interface ReportSection {
  category: string
  title: string
  summary: string
  item_count: number
  items: ReportSectionItem[]
}

export interface ReportDetail {
  id: string
  type: 'morning' | 'noon'
  date: string
  title: string
  overview: string
  sections: ReportSection[]
  content_markdown: string
  content_wechat: string
  stats: ReportStats
  created_at: string
  pushed: {
    wechat: boolean
    websocket: boolean
  }
}

export interface ReportDatesResponse {
  dates: string[]
}

export const reportApi = {
  /**
   * 获取报告列表（分页）
   */
  getReports: (params?: {
    date?: string
    report_type?: 'morning' | 'noon'
    page?: number
    page_size?: number
  }) => api.get<ReportListResponse>('/reports', { params }),

  /**
   * 获取有报告的日期列表
   */
  getReportDates: (limit: number = 30) =>
    api.get<ReportDatesResponse>('/reports/dates', { params: { limit } }),

  /**
   * 获取报告详情
   */
  getReportDetail: (reportId: string) =>
    api.get<ReportDetail>(`/reports/${reportId}`),
}

export default reportApi
