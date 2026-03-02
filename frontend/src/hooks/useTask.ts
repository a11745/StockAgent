/**
 * 任务 Hook - 任务创建与状态管理
 */

import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { useTaskStore } from '@/stores/task'
import { taskApi } from '@/api'
import { useWebSocket } from './useWebSocket'
import { TaskStatus, TaskType } from '@/api/types'
import type { CreateTaskRequest } from '@/api/types'

export function useTask() {
  const taskStore = useTaskStore()
  const { subscribeTask, isConnected } = useWebSocket({ autoConnect: false })
  
  const isCreating = ref(false)
  const isLoading = ref(false)
  
  // ==================== 计算属性 ====================
  
  const tasks = computed(() => taskStore.tasks)
  const activeTasks = computed(() => 
    taskStore.tasks.filter(t => 
      t.status === TaskStatus.RUNNING || t.status === TaskStatus.PENDING
    )
  )
  const currentTask = computed(() => taskStore.currentTask)
  const currentThoughts = computed(() => taskStore.currentThoughts)
  
  // ==================== 创建任务 ====================
  
  async function createTask(request: CreateTaskRequest): Promise<string | null> {
    isCreating.value = true
    
    try {
      const response = await taskApi.createTask(request)
      
      // 添加到 Store
      taskStore.addTask({
        task_id: response.task_id,
        task_type: request.task_type,
        status: response.status,
        ts_codes: request.ts_codes || [],
        stock_names: [],
        query: request.query,
        params: request.params,
        created_at: new Date().toISOString(),
        execution_time_ms: 0,
        llm_tokens_used: 0,
      })
      
      // 订阅任务进度
      if (isConnected.value) {
        subscribeTask(response.task_id)
      }
      
      ElMessage.success('任务已创建')
      return response.task_id
      
    } catch (error) {
      console.error('Create task failed:', error)
      return null
    } finally {
      isCreating.value = false
    }
  }
  
  // ==================== 快捷分析 ====================
  
  async function analyzeStock(tsCode: string): Promise<string | null> {
    return createTask({
      task_type: TaskType.STOCK_ANALYSIS,
      ts_codes: [tsCode],
    })
  }
  
  async function analyzeMarket(): Promise<string | null> {
    return createTask({
      task_type: TaskType.MARKET_OVERVIEW,
    })
  }
  
  async function queryAnalysis(query: string): Promise<string | null> {
    return createTask({
      task_type: TaskType.CUSTOM_QUERY,
      query,
    })
  }
  
  // ==================== 任务操作 ====================
  
  async function cancelTask(taskId: string): Promise<boolean> {
    try {
      await taskApi.cancelTask(taskId)
      taskStore.updateTaskStatus(taskId, TaskStatus.CANCELLED)
      ElMessage.success('任务已取消')
      return true
    } catch {
      return false
    }
  }
  
  async function refreshTaskList(): Promise<void> {
    isLoading.value = true
    try {
      const response = await taskApi.listTasks({ limit: 50 })
      taskStore.setTasks(response.tasks)
    } catch (error) {
      console.error('Refresh task list failed:', error)
    } finally {
      isLoading.value = false
    }
  }
  
  function selectTask(taskId: string): void {
    const task = taskStore.tasks.find(t => t.task_id === taskId)
    if (task) {
      taskStore.setCurrentTask(task)
      
      // 订阅进度更新
      if (isConnected.value && task.status === TaskStatus.RUNNING) {
        subscribeTask(taskId)
      }
    }
  }
  
  // ==================== 状态查询 ====================
  
  function getTaskStatusLabel(status: TaskStatus): string {
    const labels: Record<TaskStatus, string> = {
      [TaskStatus.PENDING]: '等待中',
      [TaskStatus.QUEUED]: '排队中',
      [TaskStatus.RUNNING]: '运行中',
      [TaskStatus.COMPLETED]: '已完成',
      [TaskStatus.FAILED]: '失败',
      [TaskStatus.CANCELLED]: '已取消',
    }
    return labels[status] || status
  }
  
  function getTaskStatusType(status: TaskStatus): 'primary' | 'success' | 'warning' | 'danger' | 'info' {
    const types: Record<TaskStatus, 'primary' | 'success' | 'warning' | 'danger' | 'info'> = {
      [TaskStatus.PENDING]: 'info',
      [TaskStatus.QUEUED]: 'info',
      [TaskStatus.RUNNING]: 'primary',
      [TaskStatus.COMPLETED]: 'success',
      [TaskStatus.FAILED]: 'danger',
      [TaskStatus.CANCELLED]: 'warning',
    }
    return types[status] || 'info'
  }
  
  return {
    // 状态
    tasks,
    activeTasks,
    currentTask,
    currentThoughts,
    isCreating,
    isLoading,
    
    // 方法
    createTask,
    analyzeStock,
    analyzeMarket,
    queryAnalysis,
    cancelTask,
    refreshTaskList,
    selectTask,
    
    // 工具
    getTaskStatusLabel,
    getTaskStatusType,
  }
}
