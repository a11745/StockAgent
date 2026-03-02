/**
 * API 类型定义
 * 
 * 与后端 Pydantic 模型严格对应
 * 参考: AgentServer/core/protocols.py
 */

// ==================== 枚举 ====================

/** 任务类型 - 对应后端 TaskType */
export enum TaskType {
  STOCK_ANALYSIS = 'stock_analysis',
  MARKET_OVERVIEW = 'market_overview',
  NEWS_SENTIMENT = 'news_sentiment',
  STRATEGY_BACKTEST = 'strategy_backtest',
  CUSTOM_QUERY = 'custom_query',
}

/** 任务状态 - 对应后端 TaskStatus */
export enum TaskStatus {
  PENDING = 'pending',
  QUEUED = 'queued',
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

/** 交易信号 - 对应后端 SignalType */
export enum SignalType {
  STRONG_BUY = 'strong_buy',
  BUY = 'buy',
  HOLD = 'hold',
  SELL = 'sell',
  STRONG_SELL = 'strong_sell',
}

// ==================== 认证 ====================

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  access_token: string
  refresh_token: string
  token_type: string
  user_id: string
  username: string
}

export interface RegisterRequest {
  username: string
  email: string
  password: string
  nickname?: string
}

export interface RegisterResponse {
  user_id: string
  username: string
  message: string
}

// ==================== 用户 ====================

export interface UserPreferences {
  theme?: 'light' | 'dark'
  language?: string
  notification_enabled?: boolean
  default_market?: string
}

export interface UserInfo {
  user_id: string
  username: string
  email: string
  nickname: string | null
  avatar: string | null
  watchlist: string[]
  preferences: UserPreferences
  is_admin: boolean
  created_at: string
  last_login: string | null
}

// ==================== 任务 ====================

export interface CreateTaskRequest {
  task_type: TaskType
  ts_codes?: string[]
  query?: string
  params?: Record<string, unknown>
  priority?: number
}

export interface CreateTaskResponse {
  task_id: string
  status: TaskStatus
  message: string
}

/** 分析评分 - 对应后端 AnalysisScore */
export interface AnalysisScore {
  fundamental?: number
  technical?: number
  sentiment?: number
  valuation?: number
}

/** 分析结果 - 对应后端 AnalysisResult */
export interface AnalysisResult {
  signal: SignalType
  confidence: number
  scores: AnalysisScore
  summary: string
  fundamental_analysis?: string
  technical_analysis?: string
  sentiment_analysis?: string
  target_price?: number
  stop_loss_price?: number
  risks?: string[]
}

/** 股票名称信息 */
export interface StockNameInfo {
  ts_code: string
  name: string
}

/** 任务详情 */
export interface Task {
  task_id: string
  task_type: TaskType
  status: TaskStatus
  ts_codes: string[]
  stock_names: StockNameInfo[]  // 股票名称信息
  query?: string
  params?: Record<string, unknown>
  created_at: string
  started_at?: string
  completed_at?: string
  result?: AnalysisResult
  error_message?: string
  execution_time_ms: number
  llm_tokens_used: number
}

export interface TaskListResponse {
  tasks: Task[]
  total: number
  limit: number
  offset: number
}

// ==================== 任务进度 (WebSocket) ====================

/** 任务进度消息 - 对应后端 TaskProgressMessage */
export interface TaskProgressMessage {
  type: 'task_progress'
  task_id: string
  status: TaskStatus
  progress: number
  current_step?: string
  message?: string
}

/** 任务完成消息 - 对应后端 ResultMessage */
export interface TaskResultMessage {
  type: 'task_completed' | 'task_failed'
  task_id: string
  status: TaskStatus
  result?: AnalysisResult
  error_message?: string
  execution_time_ms?: number
}

/** Agent 思考流消息 (打字机效果) */
export interface AgentThoughtMessage {
  type: 'agent_thought'
  task_id: string
  node_name: string
  content: string
  is_final: boolean
}

/** WebSocket 消息联合类型 */
export type WSMessage = 
  | TaskProgressMessage 
  | TaskResultMessage 
  | AgentThoughtMessage
  | { type: 'connected'; user_id: string }
  | { type: 'pong' }
  | { type: 'subscribed'; task_id: string }

// ==================== 股票 ====================

export interface StockBasic {
  ts_code: string
  symbol: string
  name: string
  area: string
  industry: string
  market: string
  list_date: string
  list_status: 'L' | 'D' | 'P'
}

export interface StockDaily {
  ts_code: string
  trade_date: string
  open: number
  high: number
  low: number
  close: number
  pre_close: number
  change: number
  pct_chg: number
  vol: number
  amount: number
}

export interface StockQuote {
  ts_code: string
  name: string
  price: number
  pct_chg: number
  vol: number
  amount: number
  high: number
  low: number
  open: number
  pre_close?: number
}

export interface MarketOverview {
  trade_date: string
  sh_index: number
  sh_change: number
  sz_index: number
  sz_change: number
  cyb_index: number
  cyb_change: number
  up_count: number
  down_count: number
  flat_count: number
  limit_up: number
  limit_down: number
  total_amount: number
  north_money: number
  hot_sectors: string[]
}

// ==================== 策略 ====================

export interface Strategy {
  strategy_id: string
  name: string
  description?: string
  is_active: boolean
  is_public: boolean
  stock_pool: string[]
  filters: Record<string, unknown>
  weights: Record<string, number>
  alert_enabled: boolean
  alert_conditions?: Record<string, unknown>
  run_count: number
  last_run_at?: string
  created_at: string
  updated_at: string
}

export interface CreateStrategyRequest {
  name: string
  description?: string
  stock_pool?: string[]
  filters?: Record<string, unknown>
  weights?: Record<string, number>
  alert_enabled?: boolean
}

// ==================== 策略订阅 (Listener 节点) ====================

/** 策略类型 - 对应后端 StrategyType */
export enum StrategyType {
  LIMIT_OPEN = 'limit_open',
  PRICE_CHANGE = 'price_change',
  VOLUME_SURGE = 'volume_surge',
  MA_CROSS = 'ma_cross',
  MA5_BUY = 'ma5_buy',
  CUSTOM = 'custom',
}

/** 策略订阅 - 对应后端 StrategySubscription */
/** 股票简要信息 */
export interface StockInfoBrief {
  ts_code: string
  name: string
}

export interface StrategySubscription {
  subscription_id: string
  strategy_id: string
  strategy_name: string
  strategy_type: StrategyType | string
  watch_list: string[]
  watch_list_info: StockInfoBrief[]  // 包含股票名称的列表
  params: Record<string, unknown>
  is_active: boolean
  user_id?: string
  created_at: string
  updated_at: string
}

/** 创建订阅请求 */
export interface CreateSubscriptionRequest {
  strategy_name: string
  strategy_type: StrategyType | string
  watch_list?: string[]
  params?: Record<string, unknown>
  is_active?: boolean
}

/** 添加股票响应 */
export interface AddStockResponse {
  success: boolean
  message: string
  watch_list: string[]
}

/** 切换订阅状态响应 */
export interface ToggleSubscriptionResponse {
  strategy_type: string
  is_active: boolean
  message: string
}

/** 策略参数定义 */
export interface StrategyParamDef {
  key: string
  label: string
  type: 'number' | 'float' | 'boolean' | 'string'
  default: number | boolean | string
}

/** 策略类型信息（服务端返回） */
export interface StrategyTypeInfo {
  type: string
  name: string
  description: string
  param_schema: StrategyParamDef[]
}
