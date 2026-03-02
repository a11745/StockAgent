<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { CandlestickChart, LineChart, BarChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
} from 'echarts/components'
import { useThemeStore } from '@/stores'
import type { StockDaily } from '@/api'

// 注册 ECharts 组件
use([
  CanvasRenderer,
  CandlestickChart,
  LineChart,
  BarChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
])

const props = defineProps<{
  data: StockDaily[]
  tsCode: string
}>()

const themeStore = useThemeStore()

// 主题相关颜色
const themeColors = computed(() => {
  const isDark = themeStore.isDark
  
  return {
    // 涨跌色
    upColor: isDark ? '#ff5a6a' : '#f23645',
    downColor: isDark ? '#26a69a' : '#089981',
    // 背景和文字
    tooltipBg: isDark ? 'rgba(30, 41, 59, 0.96)' : 'rgba(255, 255, 255, 0.96)',
    tooltipBorder: isDark ? '#475569' : '#e2e8f0',
    tooltipText: isDark ? '#f1f5f9' : '#1e293b',
    // 坐标轴
    axisLine: isDark ? '#334155' : '#e2e8f0',
    axisLabel: isDark ? '#94a3b8' : '#64748b',
    splitLine: isDark ? '#1e293b' : '#f1f5f9',
    // 图例
    legendText: isDark ? '#cbd5e1' : '#475569',
    // DataZoom
    zoomBg: isDark ? '#1e293b' : '#f1f5f9',
    zoomDataLine: isDark ? '#64748b' : '#94a3b8',
    zoomHandleBorder: isDark ? '#60a5fa' : '#3b82f6',
    zoomHandleShadow: isDark ? 'rgba(96, 165, 250, 0.4)' : 'rgba(59, 130, 246, 0.3)',
    zoomText: isDark ? '#94a3b8' : '#64748b',
    zoomMoveHandle: isDark ? '#64748b' : '#94a3b8',
  }
})

// 图表选项
const option = computed(() => {
  if (!props.data || props.data.length === 0) {
    return {}
  }
  
  const colors = themeColors.value
  
  // 反转数据（API 返回的是倒序）
  const sortedData = [...props.data].reverse()
  
  // 日期
  const dates = sortedData.map((d) => d.trade_date)
  
  // K线数据 [开, 收, 低, 高]
  const klineData = sortedData.map((d) => [d.open, d.close, d.low, d.high])
  
  // 成交量
  const volumes = sortedData.map((d) => ({
    value: d.vol,
    itemStyle: {
      color: d.close >= d.open ? colors.upColor : colors.downColor,
    },
  }))
  
  // MA 计算
  const ma5 = calculateMA(sortedData, 5)
  const ma10 = calculateMA(sortedData, 10)
  const ma20 = calculateMA(sortedData, 20)
  
  return {
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross',
        crossStyle: {
          color: colors.axisLabel,
        },
      },
      backgroundColor: colors.tooltipBg,
      borderColor: colors.tooltipBorder,
      textStyle: {
        color: colors.tooltipText,
      },
    },
    legend: {
      data: ['K线', 'MA5', 'MA10', 'MA20'],
      top: 10,
      textStyle: {
        color: colors.legendText,
      },
    },
    grid: [
      {
        left: 60,
        right: 20,
        top: 50,
        height: '55%',
      },
      {
        left: 60,
        right: 20,
        top: '72%',
        height: '18%',
      },
    ],
    xAxis: [
      {
        type: 'category',
        data: dates,
        boundaryGap: false,
        axisLine: { lineStyle: { color: colors.axisLine } },
        axisTick: { show: false },
        axisLabel: { color: colors.axisLabel },
        splitLine: { show: false },
      },
      {
        type: 'category',
        gridIndex: 1,
        data: dates,
        boundaryGap: false,
        axisLabel: { show: false },
        axisTick: { show: false },
        axisLine: { lineStyle: { color: colors.axisLine } },
        splitLine: { show: false },
      },
    ],
    yAxis: [
      {
        scale: true,
        splitArea: { show: false },
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: colors.axisLabel },
        splitLine: { lineStyle: { color: colors.splitLine } },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: { show: false },
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
      },
    ],
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: [0, 1],
        start: 70,
        end: 100,
      },
      {
        show: true,
        xAxisIndex: [0, 1],
        type: 'slider',
        bottom: 8,
        start: 70,
        end: 100,
        height: 28,
        borderColor: 'transparent',
        backgroundColor: colors.zoomBg,
        borderRadius: 6,
        // 数据阴影
        dataBackground: {
          lineStyle: {
            color: colors.zoomDataLine,
            width: 1,
          },
          areaStyle: {
            color: {
              type: 'linear',
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(59, 130, 246, 0.3)' },
                { offset: 1, color: 'rgba(59, 130, 246, 0.05)' },
              ],
            },
          },
        },
        // 选中区域
        selectedDataBackground: {
          lineStyle: {
            color: colors.zoomHandleBorder,
            width: 1.5,
          },
          areaStyle: {
            color: {
              type: 'linear',
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(59, 130, 246, 0.5)' },
                { offset: 1, color: 'rgba(59, 130, 246, 0.1)' },
              ],
            },
          },
        },
        // 填充色
        fillerColor: 'rgba(59, 130, 246, 0.15)',
        // 左右手柄
        handleIcon: 'path://M-9.5,0a9.5,9.5,0,1,0,19,0a9.5,9.5,0,1,0,-19,0M-5,0a5,5,0,1,0,10,0a5,5,0,1,0,-10,0M-2,0a2,2,0,1,0,4,0a2,2,0,1,0,-4,0',
        handleSize: '120%',
        handleStyle: {
          color: themeStore.isDark ? '#1e293b' : '#fff',
          borderColor: colors.zoomHandleBorder,
          borderWidth: 2,
          shadowBlur: 6,
          shadowColor: colors.zoomHandleShadow,
          shadowOffsetX: 0,
          shadowOffsetY: 2,
        },
        // 两端样式
        brushStyle: {
          color: 'rgba(59, 130, 246, 0.2)',
        },
        // 移动时的样式
        moveHandleSize: 6,
        moveHandleStyle: {
          color: colors.zoomMoveHandle,
          borderColor: 'transparent',
        },
        // 强调样式
        emphasis: {
          handleStyle: {
            borderColor: '#2563eb',
            shadowBlur: 10,
            shadowColor: 'rgba(59, 130, 246, 0.5)',
          },
          moveHandleStyle: {
            color: '#3b82f6',
          },
        },
        // 文本样式
        textStyle: {
          color: colors.zoomText,
          fontSize: 11,
        },
        // 边框
        brushSelect: false,
      },
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: klineData,
        itemStyle: {
          color: colors.upColor,
          color0: colors.downColor,
          borderColor: colors.upColor,
          borderColor0: colors.downColor,
        },
      },
      {
        name: 'MA5',
        type: 'line',
        data: ma5,
        smooth: true,
        lineStyle: { width: 1.5 },
        symbol: 'none',
        itemStyle: { color: '#3b82f6' },
      },
      {
        name: 'MA10',
        type: 'line',
        data: ma10,
        smooth: true,
        lineStyle: { width: 1.5 },
        symbol: 'none',
        itemStyle: { color: '#22c55e' },
      },
      {
        name: 'MA20',
        type: 'line',
        data: ma20,
        smooth: true,
        lineStyle: { width: 1.5 },
        symbol: 'none',
        itemStyle: { color: '#f59e0b' },
      },
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
      },
    ],
  }
})

// 计算移动平均线
function calculateMA(data: StockDaily[], period: number): (number | '-')[] {
  const result: (number | '-')[] = []
  
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      result.push('-')
    } else {
      let sum = 0
      for (let j = 0; j < period; j++) {
        sum += data[i - j].close
      }
      result.push(Number((sum / period).toFixed(2)))
    }
  }
  
  return result
}
</script>

<template>
  <div class="stock-chart">
    <VChart v-if="data.length > 0" :option="option" autoresize />
    <el-empty v-else description="暂无K线数据" />
  </div>
</template>

<style lang="scss" scoped>
.stock-chart {
  width: 100%;
  height: 100%;
  min-height: 400px;
}
</style>
