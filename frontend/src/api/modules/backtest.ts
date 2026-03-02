/**
 * 回测 API
 */

import { api } from '../client'

// ==================== 类型定义 ====================

export interface BacktestRequest {
  ts_code: string
  stock_name?: string
  start_date: string
  end_date: string
  initial_cash?: number
  entry_threshold?: number
  exit_threshold?: number
  position_size?: number
  factor_weights?: Record<string, number>
  auto_technical?: boolean
}

export interface BacktestTaskResponse {
  task_id: string
  status: string
  message: string
}

export interface BacktestStatus {
  task_id: string
  status: 'pending' | 'queued' | 'running' | 'completed' | 'failed' | 'cancelled'
  created_at?: string
  started_at?: string
  completed_at?: string
  error?: string
}

export interface BacktestTrade {
  date: string
  direction: 'buy' | 'sell'
  price: number
  shares: number
  amount: number
  commission: number
  stamp_duty?: number
  reason: string
}

export interface BacktestMetrics {
  returns: {
    total_return_pct: number
    annual_return_pct: number
    benchmark_return_pct: number
    alpha_pct: number
  }
  risk: {
    volatility_pct: number
    max_drawdown_pct: number
    sharpe_ratio: number
    sortino_ratio: number
    calmar_ratio: number
  }
  trades: {
    total_trades: number
    win_rate_pct: number
    profit_factor: number
    avg_profit: number
    avg_loss: number
    max_consecutive_wins: number
    max_consecutive_losses: number
  }
  exposure: {
    total_days: number
    days_in_market: number
    market_exposure_pct: number
    avg_holding_days: number
  }
  costs: {
    total_commission: number
    total_stamp_duty: number
    total_costs: number
  }
}

export interface BacktestCharts {
  nav_series: {
    dates: string[]
    strategy: number[]
    benchmark: number[]
  }
  drawdown_series: {
    dates: string[]
    values: number[]
  }
}

export interface BacktestResult {
  task_id: string
  status: string
  result?: {
    summary: {
      ts_code: string
      start_date: string
      end_date: string
      initial_cash: number
      final_equity: number
      execution_time_ms: number
    }
    metrics: BacktestMetrics
    charts: BacktestCharts
    trades: BacktestTrade[]
  }
  error?: string
}

export interface BacktestHistoryItem {
  task_id: string
  task_type?: 'single' | 'factor_selection'
  start_date: string
  end_date: string
  created_at: string
  total_return_pct?: number
  sharpe_ratio?: number
  max_drawdown_pct?: number
  // 单股回测字段
  ts_code?: string
  stock_name?: string
  // 因子选股字段
  top_n?: number
  rebalance_freq?: string
  factors_count?: number
  excess_return_pct?: number
}

// ==================== 因子选股类型 ====================

export interface FactorConfig {
  name: string
  weight: number
  direction?: string
}

export interface FactorSelectionRequest {
  universe?: string
  start_date: string
  end_date: string
  initial_cash?: number
  rebalance_freq?: string
  top_n?: number
  weight_method?: string
  factors: FactorConfig[]
  exclude?: string[]
  benchmark?: string
}

export interface FactorInfo {
  name: string
  display_name: string
  category: string
  description: string
  direction: string
  data_source: string
}

export interface FactorListResponse {
  factors: FactorInfo[]
  grouped: Record<string, FactorInfo[]>
}

// ==================== API 方法 ====================

/**
 * 提交回测任务
 */
export async function submitBacktest(request: BacktestRequest): Promise<BacktestTaskResponse> {
  return api.post<BacktestTaskResponse>('/backtest/submit', request)
}

/**
 * 查询回测任务状态
 */
export async function getBacktestStatus(taskId: string): Promise<BacktestStatus> {
  return api.get<BacktestStatus>(`/backtest/status/${taskId}`)
}

/**
 * 获取回测结果
 */
export async function getBacktestResult(taskId: string): Promise<BacktestResult> {
  return api.get<BacktestResult>(`/backtest/result/${taskId}`)
}

/**
 * 获取回测历史
 * @param taskType 任务类型: 'single' | 'factor_selection' | undefined
 */
export async function getBacktestHistory(
  limit: number = 20,
  offset: number = 0,
  taskType?: 'single' | 'factor_selection'
): Promise<{ total: number; items: BacktestHistoryItem[] }> {
  const params: Record<string, unknown> = { limit, offset }
  if (taskType) {
    params.task_type = taskType
  }
  return api.get<{ total: number; items: BacktestHistoryItem[] }>(
    '/backtest/history',
    { params }
  )
}

/**
 * 取消回测任务
 */
export async function cancelBacktest(taskId: string): Promise<{ task_id: string; status: string }> {
  return api.delete<{ task_id: string; status: string }>(`/backtest/${taskId}`)
}

/**
 * 获取可用因子列表
 */
export async function getFactors(): Promise<FactorListResponse> {
  return api.get<FactorListResponse>('/backtest/factors')
}

/**
 * 提交因子选股回测
 */
export async function submitFactorSelection(request: FactorSelectionRequest): Promise<BacktestTaskResponse> {
  return api.post<BacktestTaskResponse>('/backtest/factor-selection', request)
}

export default {
  submitBacktest,
  getBacktestStatus,
  getBacktestResult,
  getBacktestHistory,
  cancelBacktest,
  getFactors,
  submitFactorSelection,
}
