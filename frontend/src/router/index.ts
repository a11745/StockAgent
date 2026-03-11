/**
 * Vue Router 配置
 */

import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'

// ==================== 路由定义 ====================

const routes: RouteRecordRaw[] = [
  // 认证相关
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/auth/LoginView.vue'),
    meta: { title: '登录', guest: true },
  },
  {
    path: '/register',
    name: 'Register',
    component: () => import('@/views/auth/RegisterView.vue'),
    meta: { title: '注册', guest: true },
  },
  
  // 主布局
  {
    path: '/',
    component: () => import('@/layouts/MainLayout.vue'),
    meta: { requiresAuth: true },
    children: [
      {
        path: '',
        redirect: '/dashboard',
      },
      {
        path: 'dashboard',
        name: 'Dashboard',
        component: () => import('@/views/dashboard/DashboardView.vue'),
        meta: { title: '仪表盘' },
      },
      
      // 分析
      {
        path: 'analysis',
        name: 'AnalysisList',
        component: () => import('@/views/analysis/AnalysisListView.vue'),
        meta: { title: '分析任务' },
      },
      {
        path: 'analysis/:id',
        name: 'AnalysisDetail',
        component: () => import('@/views/analysis/AnalysisDetailView.vue'),
        meta: { title: '分析详情' },
      },
      
      // 股票
      {
        path: 'stock/:code',
        name: 'StockDetail',
        component: () => import('@/views/stock/StockDetailView.vue'),
        meta: { title: '个股详情' },
      },
      
      // 行情分析
      {
        path: 'market',
        name: 'MarketAnalysis',
        component: () => import('@/views/market/MarketAnalysisView.vue'),
        meta: { title: '行情分析' },
      },
      
      // 板块策略
      {
        path: 'sector-strategy',
        name: 'SectorStrategy',
        component: () => import('@/views/market/SectorStrategyView.vue'),
        meta: { title: '板块分析' },
      },
      
      // 热点追踪
      {
        path: 'hot-news',
        name: 'HotNews',
        component: () => import('@/views/market/HotNewsView.vue'),
        meta: { title: '热点追踪' },
      },
      
      // 报告回顾
      {
        path: 'reports',
        name: 'Reports',
        component: () => import('@/views/report/ReportView.vue'),
        meta: { title: '报告回顾' },
      },
      
      // 量化回测 - 单股回测
      {
        path: 'backtest',
        name: 'Backtest',
        component: () => import('@/views/backtest/BacktestView.vue'),
        meta: { title: '量化回测' },
      },
      
      // 量化回测 - 因子选股
      {
        path: 'factor-selection',
        name: 'FactorSelection',
        component: () => import('@/views/backtest/FactorSelectionView.vue'),
        meta: { title: '因子选股' },
      },
      
      // 自选股
      {
        path: 'watchlist',
        name: 'Watchlist',
        component: () => import('@/views/watchlist/WatchlistView.vue'),
        meta: { title: '自选股' },
      },
      
      // 策略
      {
        path: 'strategies',
        name: 'StrategyList',
        component: () => import('@/views/strategy/StrategyListView.vue'),
        meta: { title: '市场监听' },
      },
      {
        path: 'strategies/new',
        name: 'StrategyCreate',
        component: () => import('@/views/strategy/StrategyEditView.vue'),
        meta: { title: '创建策略' },
      },
      {
        path: 'strategies/:id',
        name: 'StrategyDetail',
        component: () => import('@/views/strategy/StrategyDetailView.vue'),
        meta: { title: '策略详情' },
      },
      {
        path: 'strategies/:id/edit',
        name: 'StrategyEdit',
        component: () => import('@/views/strategy/StrategyEditView.vue'),
        meta: { title: '编辑策略' },
      },
      
      // 设置
      {
        path: 'settings',
        name: 'Settings',
        component: () => import('@/views/settings/SettingsView.vue'),
        meta: { title: '设置' },
      },
    ],
  },
  
  // 404
  {
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: () => import('@/views/error/NotFoundView.vue'),
    meta: { title: '页面不存在' },
  },
]

// ==================== 创建 Router ====================

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
  scrollBehavior() {
    return { top: 0 }
  },
})

// ==================== 路由守卫 ====================

router.beforeEach((to, _from, next) => {
  // 设置页面标题
  const title = to.meta.title as string
  if (title) {
    document.title = `${title} - StockAgent`
  }
  
  const token = localStorage.getItem('access_token')
  const requiresAuth = to.matched.some(record => record.meta.requiresAuth)
  const isGuestOnly = to.matched.some(record => record.meta.guest)
  
  if (requiresAuth && !token) {
    // 需要登录但未登录
    next({ 
      path: '/login', 
      query: { redirect: to.fullPath },
    })
  } else if (isGuestOnly && token) {
    // 已登录但访问登录/注册页
    next('/dashboard')
  } else {
    next()
  }
})

export default router
