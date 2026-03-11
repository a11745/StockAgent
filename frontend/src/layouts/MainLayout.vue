<script setup lang="ts">
/**
 * 主布局 - 包含侧边栏和顶部导航
 * 支持 Light/Dark 主题切换
 */

import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  ElContainer,
  ElAside,
  ElHeader,
  ElMain,
  ElMenu,
  ElMenuItem,
  ElDropdown,
  ElDropdownMenu,
  ElDropdownItem,
  ElAvatar,
  ElBadge,
  ElIcon,
  ElTooltip,
} from 'element-plus'
import {
  HomeFilled,
  DataAnalysis,
  TrendCharts,
  Star,
  Setting,
  Fold,
  Expand,
  User,
  SwitchButton,
  Bell,
  Histogram,
  Sunny,
  Moon,
  Promotion,
  Document,
} from '@element-plus/icons-vue'
import { useAuth } from '@/hooks'
import { useUserStore, useTaskStore, useThemeStore } from '@/stores'

const route = useRoute()
const router = useRouter()
const { logout } = useAuth()
const userStore = useUserStore()
const taskStore = useTaskStore()
const themeStore = useThemeStore()

// ==================== 状态 ====================

const isCollapsed = ref(false)

// ==================== 计算属性 ====================

const activeMenu = computed(() => {
  const path = route.path
  if (path.startsWith('/analysis')) return '/analysis'
  if (path.startsWith('/stock')) return '/analysis'
  if (path.startsWith('/strategies')) return '/strategies'
  return path
})

const activeTaskCount = computed(() => taskStore.activeTaskCount)

// ==================== 菜单项 ====================

const menuItems = [
  { path: '/dashboard', icon: HomeFilled, title: '仪表盘' },
  { path: '/market', icon: Histogram, title: '行情分析' },
  { path: '/sector-strategy', icon: TrendCharts, title: '板块分析' },
  { path: '/hot-news', icon: Promotion, title: '热点追踪' },
  { path: '/reports', icon: Document, title: '报告回顾' },
  { path: '/backtest', icon: DataAnalysis, title: '单股回测' },
  { path: '/factor-selection', icon: DataAnalysis, title: '因子选股' },
  { path: '/analysis', icon: DataAnalysis, title: '分析任务' },
  { path: '/watchlist', icon: Star, title: '自选股' },
  { path: '/strategies', icon: TrendCharts, title: '市场监听' },
  { path: '/settings', icon: Setting, title: '设置' },
]

// ==================== 生命周期 ====================

onMounted(() => {
  // 初始化主题
  themeStore.initTheme()
})

// ==================== 方法 ====================

function handleMenuSelect(path: string): void {
  router.push(path)
}

function toggleCollapse(): void {
  isCollapsed.value = !isCollapsed.value
}

function toggleTheme(): void {
  themeStore.toggleTheme()
}

async function handleLogout(): Promise<void> {
  await logout()
}
</script>

<template>
  <ElContainer class="main-layout">
    <!-- 侧边栏 -->
    <ElAside :width="isCollapsed ? '64px' : '220px'" class="sidebar">
      <div class="logo" :class="{ collapsed: isCollapsed }">
        <img src="/logo.svg" alt="Logo" class="logo-img" />
        <span v-if="!isCollapsed" class="logo-text">StockAgent</span>
      </div>
      
      <ElMenu
        :default-active="activeMenu"
        :collapse="isCollapsed"
        class="sidebar-menu"
        @select="handleMenuSelect"
      >
        <ElMenuItem
          v-for="item in menuItems"
          :key="item.path"
          :index="item.path"
          class="menu-item-wrapper"
        >
          <ElIcon><component :is="item.icon" /></ElIcon>
          <template #title>
            <div class="menu-title-wrapper">
              <span>{{ item.title }}</span>
              <span 
                v-if="item.path === '/analysis' && activeTaskCount > 0"
                class="menu-badge-dot"
              >
                {{ activeTaskCount > 99 ? '99+' : activeTaskCount }}
              </span>
            </div>
          </template>
        </ElMenuItem>
      </ElMenu>
      
      <div class="collapse-btn" @click="toggleCollapse">
        <ElIcon :size="20">
          <Fold v-if="!isCollapsed" />
          <Expand v-else />
        </ElIcon>
      </div>
    </ElAside>
    
    <!-- 主内容区 -->
    <ElContainer class="main-container">
      <!-- 顶部栏 -->
      <ElHeader class="header">
        <div class="header-left">
          <h2 class="page-title">{{ $route.meta.title }}</h2>
        </div>
        
        <div class="header-right">
          <!-- 主题切换 -->
          <ElTooltip :content="themeStore.isDark ? '切换到浅色模式' : '切换到深色模式'" placement="bottom">
            <button class="theme-toggle" @click="toggleTheme">
              <Transition name="theme-icon" mode="out-in">
                <Moon v-if="themeStore.isDark" key="dark" class="theme-icon" />
                <Sunny v-else key="light" class="theme-icon" />
              </Transition>
            </button>
          </ElTooltip>
          
          <!-- 通知 -->
          <ElBadge :value="activeTaskCount" :hidden="activeTaskCount === 0" class="notification">
            <ElIcon :size="20"><Bell /></ElIcon>
          </ElBadge>
          
          <!-- 用户菜单 -->
          <ElDropdown trigger="click" @command="handleMenuSelect">
            <div class="user-info">
              <ElAvatar :size="32" :src="userStore.avatar || undefined">
                {{ userStore.nickname.charAt(0) }}
              </ElAvatar>
              <span class="username">{{ userStore.nickname }}</span>
            </div>
            <template #dropdown>
              <ElDropdownMenu>
                <ElDropdownItem :icon="User" command="/settings">
                  个人中心
                </ElDropdownItem>
                <ElDropdownItem :icon="Setting" command="/settings">
                  设置
                </ElDropdownItem>
                <ElDropdownItem divided :icon="SwitchButton" @click="handleLogout">
                  退出登录
                </ElDropdownItem>
              </ElDropdownMenu>
            </template>
          </ElDropdown>
        </div>
      </ElHeader>
      
      <!-- 内容区 -->
      <ElMain class="main-content">
        <RouterView />
      </ElMain>
    </ElContainer>
  </ElContainer>
</template>

<style scoped lang="scss">
.main-layout {
  min-height: 100vh;
  overflow: hidden;
}

.sidebar {
  background: var(--sidebar-bg);
  display: flex;
  flex-direction: column;
  transition: width 0.3s;
  position: fixed;
  top: 0;
  left: 0;
  height: 100vh;
  z-index: 100;
  
  .logo {
    height: 60px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    padding: 0 16px;
    border-bottom: 1px solid var(--sidebar-border);
    
    .logo-img {
      width: 32px;
      height: 32px;
    }
    
    .logo-text {
      font-size: 18px;
      font-weight: 700;
      color: white;
      white-space: nowrap;
    }
    
    &.collapsed {
      padding: 0;
      
      .logo-img {
        width: 28px;
        height: 28px;
      }
    }
  }
  
  .sidebar-menu {
    flex: 1;
    border-right: none;
    background: transparent;
    overflow-y: auto;
    
    :deep(.el-menu-item) {
      color: var(--sidebar-text);
      
      &:hover {
        background: var(--sidebar-hover-bg);
        color: var(--sidebar-text-active);
      }
      
      &.is-active {
        background: var(--sidebar-active-bg);
        color: var(--sidebar-text-active);
      }
    }
    
    :deep(.menu-title-wrapper) {
      display: flex;
      align-items: center;
      justify-content: space-between;
      width: 100%;
      padding-right: 8px;
    }
    
    :deep(.menu-badge-dot) {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 18px;
      height: 18px;
      padding: 0 5px;
      font-size: 11px;
      font-weight: 600;
      color: #fff;
      background: var(--error);
      border-radius: 9px;
      margin-left: 8px;
      flex-shrink: 0;
    }
  }
  
  .collapse-btn {
    height: 48px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--sidebar-text);
    cursor: pointer;
    transition: all 0.3s;
    border-top: 1px solid var(--sidebar-border);
    
    &:hover {
      color: var(--sidebar-text-active);
      background: var(--sidebar-hover-bg);
    }
  }
}

.main-container {
  flex-direction: column;
  background: var(--bg-base);
  margin-left: v-bind("isCollapsed ? '64px' : '220px'");
  transition: margin-left 0.3s, background-color var(--transition-normal);
  height: 100vh;
  overflow-y: auto;
}

.header {
  background: var(--header-bg);
  border-bottom: 1px solid var(--header-border);
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
  position: sticky;
  top: 0;
  z-index: 50;
  transition: background-color var(--transition-normal), border-color var(--transition-normal);
  
  .header-left {
    .page-title {
      margin: 0;
      font-size: 18px;
      font-weight: 600;
      color: var(--text-primary);
    }
  }
  
  .header-right {
    display: flex;
    align-items: center;
    gap: 16px;
    
    .theme-toggle {
      width: 36px;
      height: 36px;
      display: flex;
      align-items: center;
      justify-content: center;
      background: var(--bg-muted);
      border: 1px solid var(--border-default);
      border-radius: var(--radius-md);
      cursor: pointer;
      transition: all var(--transition-fast);
      
      .theme-icon {
        width: 18px;
        height: 18px;
        color: var(--text-secondary);
        transition: color var(--transition-fast);
      }
      
      &:hover {
        background: var(--bg-hover);
        border-color: var(--primary-400);
        
        .theme-icon {
          color: var(--primary-500);
        }
      }
    }
    
    .notification {
      cursor: pointer;
      color: var(--text-secondary);
      
      &:hover {
        color: var(--primary-500);
      }
    }
    
    .user-info {
      display: flex;
      align-items: center;
      gap: 8px;
      cursor: pointer;
      padding: 4px 8px;
      border-radius: var(--radius-md);
      transition: background var(--transition-fast);
      
      &:hover {
        background: var(--bg-hover);
      }
      
      .username {
        font-weight: 500;
        color: var(--text-primary);
      }
    }
  }
}

.main-content {
  padding: 0;
  flex: 1;
  overflow-x: hidden;
  background: var(--bg-base);
  transition: background-color var(--transition-normal);
}

// 主题图标切换动画
.theme-icon-enter-active,
.theme-icon-leave-active {
  transition: all 0.2s ease;
}

.theme-icon-enter-from {
  opacity: 0;
  transform: rotate(-90deg) scale(0.8);
}

.theme-icon-leave-to {
  opacity: 0;
  transform: rotate(90deg) scale(0.8);
}
</style>
