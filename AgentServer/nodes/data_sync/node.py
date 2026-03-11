"""
数据同步节点实现

使用分布式锁防止多个节点重复抓取同一天的行情数据。
使用 BulkWrite 批量写入提高同步效率。
"""

import asyncio
from typing import Optional, List, Type, Union

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core.base import BaseNode, ScheduledJob, BaseCollector, BaseTask, BaseGenerator
from core.protocols import NodeType
from core.managers import redis_manager, mongo_manager, data_source_manager, llm_manager, milvus_manager

# 数据采集器 (Collectors)
from .collectors import (
    StockBasicCollector,
    StockDailyCollector,
    DailyBasicCollector,
    IndexBasicCollector,
    IndexDailyCollector,
    MoneyflowIndustryCollector,
    MoneyflowConceptCollector,
    LimitListCollector,
    StockNewsCollector,
    FinaIndicatorCollector,
    HotNewsCollector,
    MultiSourceCollector,
)

# 处理任务 (Tasks)
from .tasks import (
    DailyStatsTask,
    EventClusteringTask,
    NewsLifecycleTask,
)

# 生成任务 (Generators)
from .generators import (
    MorningReportGenerator,
    NoonReportGenerator,
)


class DataSyncNode(BaseNode):
    """
    数据同步节点
    
    职责:
    - 定时从 Tushare 同步股票基础数据、日线数据
    - 同步新闻舆情数据
    - 将数据存储到 MongoDB
    
    特性:
    - 分布式锁防止多节点重复抓取
    - BulkWrite 批量写入
    - 通过 gRPC RPC 接收远程调用
    """
    
    node_type = NodeType.DATA_SYNC
    DEFAULT_RPC_PORT = 50054  # DataSyncNode 默认 RPC 端口
    
    def __init__(self, node_id: Optional[str] = None, rpc_port: int = 0):
        from core.settings import settings
        super().__init__(node_id, rpc_port or settings.rpc.data_sync_port)
        
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._jobs: List[ScheduledJob] = []
    
    async def start(self) -> None:
        """启动数据同步节点"""
        # 按依赖顺序初始化管理器
        self.logger.info("Initializing managers...")
        await redis_manager.initialize()      # 心跳注册 + 分布式锁
        await mongo_manager.initialize()      # 数据存储
        await data_source_manager.initialize()  # 统一数据源管理
        await llm_manager.initialize()        # LLM (事件聚类)
        await milvus_manager.initialize()     # 向量存储 (事件聚类)
        
        # 启动 RPC 服务器
        await self._start_rpc_server()
        
        # 注册定时任务
        self._register_jobs()
        
        # 创建调度器
        self._scheduler = AsyncIOScheduler()
        
        # 注册采集任务
        for job in self._jobs:
            self._schedule_job(job)
        
        # 启动调度器
        self._scheduler.start()
        
        self.logger.info(f"Data sync node started with {len(self._jobs)} jobs")
    
    async def stop(self) -> None:
        """停止节点"""
        if self._scheduler:
            self._scheduler.shutdown()
    
    async def run(self) -> None:
        """节点主循环"""
        # 首次启动时执行一次同步
        if self.settings.debug:
            self.logger.info("Running initial sync...")
            await self._run_all_jobs()
        
        # 保持运行
        while self._running:
            await asyncio.sleep(60)
    
    def _register_jobs(self) -> None:
        """注册所有定时任务"""
        job_classes: List[Type[ScheduledJob]] = [
            # 数据采集 (Collectors)
            StockBasicCollector,
            StockDailyCollector,
            DailyBasicCollector,
            IndexBasicCollector,
            IndexDailyCollector,
            MoneyflowIndustryCollector,
            MoneyflowConceptCollector,
            LimitListCollector,
            StockNewsCollector,
            FinaIndicatorCollector,
            HotNewsCollector,
            MultiSourceCollector,
            
            # 处理任务 (Tasks)
            DailyStatsTask,         # 统计，确保依赖数据已同步
            EventClusteringTask,    # 事件聚类 (LLM 深度去重)
            NewsLifecycleTask,      # 数据生命周期管理
            
            # 生成任务 (Generators)
            MorningReportGenerator,  # 早报 (8:50)
            NoonReportGenerator,     # 午报 (13:50)
        ]
        
        for cls in job_classes:
            job = cls()
            self._jobs.append(job)
            self.logger.info(f"Registered job: {job.name}")
    
    def _schedule_job(self, job: ScheduledJob) -> None:
        """调度任务"""
        async def run_job():
            await self._run_job_with_lock(job)
        
        # 解析 cron 表达式
        trigger = CronTrigger.from_crontab(job.schedule)
        
        self._scheduler.add_job(
            run_job,
            trigger=trigger,
            id=job.name,
            name=f"Job: {job.name}",
            replace_existing=True,
        )
    
    async def _run_job_with_lock(self, job: ScheduledJob) -> dict:
        """
        使用分布式锁运行任务
        
        防止多个节点重复执行同一任务。
        """
        from datetime import date
        
        today = date.today().strftime("%Y%m%d")
        lock_key = f"sync:{job.name}:{today}"
        
        # 尝试获取锁
        lock = await redis_manager.try_lock(lock_key, timeout=600)  # 10 分钟超时
        
        if lock is None:
            self.logger.info(
                f"Job {job.name} skipped: "
                f"another node is running (lock={lock_key})"
            )
            return {"success": False, "skipped": True, "reason": "lock_held"}
        
        try:
            self.logger.info(f"Running job: {job.name} (lock acquired)")
            result = await job.run()
            
            if result["success"]:
                self.logger.info(
                    f"Job {job.name} completed: "
                    f"{result['count']} records, {result['duration_ms']:.2f}ms"
                )
            else:
                self.logger.error(
                    f"Job {job.name} failed: {result.get('error')}"
                )
            
            return result
            
        finally:
            # 释放锁
            await lock.release()
            self.logger.debug(f"Lock released: {lock_key}")
    
    async def _run_all_jobs(self) -> None:
        """运行所有任务（跳过 run_at_startup=False 的任务）"""
        for job in self._jobs:
            # 跳过不在启动时运行的任务
            if not getattr(job, 'run_at_startup', True):
                self.logger.info(f"Job {job.name} skipped (run_at_startup=False)")
                continue
            
            try:
                await self._run_job_with_lock(job)
            except Exception as e:
                self.logger.exception(f"Job {job.name} error: {e}")
    
    async def run_job(self, job_name: str) -> dict:
        """手动运行指定任务"""
        for job in self._jobs:
            if job.name == job_name:
                return await self._run_job_with_lock(job)
        
        return {"success": False, "error": f"Job not found: {job_name}"}
    
    def get_job_status(self) -> List[dict]:
        """获取所有任务状态"""
        return [j.status for j in self._jobs]
    
    # ==================== RPC 方法 ====================
    
    def _register_rpc_methods(self) -> None:
        """注册 RPC 方法"""
        super()._register_rpc_methods()
        
        # 注册热点新闻刷新方法
        self.register_rpc_method("refresh_hot_news", self._handle_refresh_hot_news)
        self.logger.info("Registered RPC method: refresh_hot_news")
    
    async def _handle_refresh_hot_news(self, params: dict) -> dict:
        """
        处理热点新闻刷新 RPC 请求
        
        Args:
            params: {"source": "cls"} 或 {} 刷新全部
            
        Returns:
            刷新结果
        """
        source_id = params.get("source")
        trace_id = params.get("_trace_id", "-")
        
        self.logger.info(f"[{trace_id}] RPC refresh_hot_news: source={source_id or 'ALL'}")
        
        # 从已注册的任务中获取热点新闻采集器
        hot_news_collector = self._get_job("hot_news")
        if not hot_news_collector:
            return {"success": False, "error": "HotNewsCollector not found"}
        
        try:
            result = await hot_news_collector.refresh(source_id)
            self.logger.info(f"[{trace_id}] refresh_hot_news done: {result}")
            return result
        except Exception as e:
            self.logger.exception(f"[{trace_id}] refresh_hot_news failed: {e}")
            return {
                "success_count": 0,
                "fail_count": 1,
                "total_news": 0,
                "error": str(e),
            }
    
    def _get_job(self, name: str) -> Optional[ScheduledJob]:
        """根据名称获取任务"""
        for job in self._jobs:
            if job.name == name:
                return job
        return None


def main():
    """入口函数"""
    node = DataSyncNode()
    asyncio.run(node.main())


if __name__ == "__main__":
    main()
