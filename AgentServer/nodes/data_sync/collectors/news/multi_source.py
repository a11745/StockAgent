"""
多源新闻聚合采集器

整合多个新闻源，提供统一的采集接口。
支持：财经快讯、政策文件、财经讨论等多种新闻源。

调度策略:
- 调度器每分钟触发一次
- 内部根据分组配置的间隔时间决定是否采集
- 不同分组有不同的采集频率
- 采集时间戳持久化到 MongoDB，重启后不会重复采集
"""

import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Type, Any, Optional

from core.base import BaseCollector
from core.settings import settings
from core.managers import mongo_manager
from src.collector.collector import BaseNewsCollector
from src.collector.sources.base import BaseSource
from src.collector.types import CollectResult

from .sources import (
    CLSSource,
    MIITSource,
    WallstreetcnSource,
    Jin10Source,
    XueqiuSource,
    EastMoneySource,
    GovSource,
    JuejinSource,
    ThePaperSource,
)


class _MultiSourceNewsCollector(BaseNewsCollector):
    """
    多源新闻采集器内部实现
    
    继承 BaseNewsCollector 获得采集功能。
    """
    
    SOURCE_CLASSES: Dict[str, Type[BaseSource]] = {
        # 财经快讯 (高频)
        "cls": CLSSource,
        # "wallstreetcn": WallstreetcnSource,
        # "jin10": Jin10Source,
        # 财经讨论 (中频)
        # "xueqiu": XueqiuSource,
        # "eastmoney": EastMoneySource,
        # 政策文件 (低频)
        "miit": MIITSource,
        "gov": GovSource,
        # 科技/综合 (中频)
        # "juejin": JuejinSource,
        "thepaper": ThePaperSource,
    }
    
    SOURCE_GROUPS: Dict[str, List[str]] = {
        "finance_flash": ["cls", "wallstreetcn", "jin10"],  # 财经快讯
        "finance_discuss": ["xueqiu", "eastmoney"],          # 财经讨论
        "policy": ["miit", "gov"],                           # 政策文件
        "tech_general": ["juejin", "thepaper"],              # 科技/综合
    }


# 分组采集间隔 (分钟)
GROUP_INTERVALS: Dict[str, int] = {
    "finance_flash": 5,     # 财经快讯: 5分钟
    "finance_discuss": 10,  # 财经讨论: 10分钟
    "policy": 1440,         # 政策文件: 每天一次 (1440分钟)
    "tech_general": 60,     # 科技/综合: 60分钟
}

# 分组时间过滤范围 (小时) - 只获取该时间范围内的新闻
GROUP_SINCE_HOURS: Dict[str, int] = {
    "finance_flash": 1,     # 财经快讯: 过去 1 小时
    "finance_discuss": 2,   # 财经讨论: 过去 2 小时
    "policy": 168,          # 政策文件: 过去 7 天 (168小时)
    "tech_general": 24,     # 科技/综合: 过去 24 小时
}


class MultiSourceCollector(BaseCollector):
    """
    多源新闻聚合采集器
    
    整合多个采集源，按分组差异化调度：
    
    财经快讯 (每 3 分钟):
    - 财联社 (电报 + 头条)
    - 华尔街见闻 (快讯)
    - 金十数据 (快讯)
    
    财经讨论 (每 5 分钟):
    - 雪球 (热股讨论)
    - 东方财富 (财富号)
    
    政策文件 (每 30 分钟):
    - 工信部 (政策/公告/标准)
    - 国务院 (政策文库)
    
    科技/综合 (每 10 分钟):
    - 稀土掘金 (技术热榜)
    - 澎湃新闻 (综合热榜)
    
    Example:
        collector = MultiSourceCollector()
        
        # 被调度器调用 (每分钟)
        result = await collector.run()
        
        # 手动采集指定分组
        result = await collector.collect_group("finance_flash")
        
        # 手动采集指定源
        result = await collector.collect_source("cls")
    """
    
    name = "multi_source_news"
    description = "多源新闻聚合采集"
    default_schedule = "* * * * *"  # 每分钟检查一次
    
    # MongoDB 集合名称
    METADATA_COLLECTION = "collector_metadata"
    
    def __init__(self):
        super().__init__()
        self._inner = _MultiSourceNewsCollector()
        # 内存缓存，启动时从 MongoDB 加载
        self._last_collect_time: Dict[str, datetime] = {}
        self._metadata_loaded = False
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间"""
        return getattr(settings.data_sync, "multi_source_news_schedule", None) or self.default_schedule
    
    async def _load_metadata(self) -> None:
        """从 MongoDB 加载采集元数据"""
        if self._metadata_loaded:
            return
        
        try:
            docs = await mongo_manager.find_many(
                self.METADATA_COLLECTION,
                {"type": "group_collect_time"},
            )
            for doc in docs:
                group = doc.get("group")
                last_time = doc.get("last_collect_time")
                if group and last_time:
                    self._last_collect_time[group] = last_time
            
            self._metadata_loaded = True
            self.logger.info(
                f"Loaded collect metadata: {list(self._last_collect_time.keys())}"
            )
        except Exception as e:
            self.logger.warning(f"Failed to load metadata: {e}")
    
    async def _save_collect_time(self, group: str, collect_time: datetime) -> None:
        """保存采集时间到 MongoDB"""
        try:
            await mongo_manager.update_one(
                self.METADATA_COLLECTION,
                {"type": "group_collect_time", "group": group},
                {"last_collect_time": collect_time},
                upsert=True,
            )
            # 同步更新内存缓存
            self._last_collect_time[group] = collect_time
        except Exception as e:
            self.logger.warning(f"Failed to save collect time for {group}: {e}")
    
    async def _should_collect_group(self, group: str) -> bool:
        """判断分组是否需要采集"""
        # 确保元数据已加载
        await self._load_metadata()
        
        interval_minutes = GROUP_INTERVALS.get(group, 10)
        last_time = self._last_collect_time.get(group)
        
        if last_time is None:
            return True
        
        elapsed = (datetime.utcnow() - last_time).total_seconds() / 60
        return elapsed >= interval_minutes
    
    async def collect(self) -> Dict[str, Any]:
        """
        执行采集
        
        检查每个分组是否到达采集时间，只采集需要更新的分组。
        """
        trace_id = uuid.uuid4().hex[:8]
        
        # 找出需要采集的分组
        groups_to_collect = []
        for group in GROUP_INTERVALS.keys():
            if await self._should_collect_group(group):
                groups_to_collect.append(group)
        
        if not groups_to_collect:
            self.logger.debug(f"[{trace_id}] No groups need collection at this time")
            return {"count": 0, "groups_collected": []}
        
        self.logger.info(
            f"[{trace_id}] Collecting groups: {groups_to_collect}"
        )
        
        total_result = {
            "count": 0,
            "total_fetched": 0,
            "duplicate_count": 0,
            "groups_collected": [],
            "errors": [],
        }
        
        for group in groups_to_collect:
            try:
                # 优先使用上次采集时间，没有则使用固定时间范围
                last_time = self._last_collect_time.get(group)
                if last_time:
                    # 使用上次采集时间作为起点
                    result = await self._inner.collect_group(
                        group=group,
                        since=last_time,
                        limit_per_source=50,
                        save_to_db=True,
                        trace_id=trace_id,
                    )
                else:
                    # 首次采集，使用固定时间范围
                    since_hours = GROUP_SINCE_HOURS.get(group, 1)
                    result = await self._inner.collect_group(
                        group=group,
                        since_hours=since_hours,
                        limit_per_source=50,
                        save_to_db=True,
                        trace_id=trace_id,
                    )
                
                # 更新上次采集时间 (持久化到 MongoDB)
                await self._save_collect_time(group, datetime.utcnow())
                
                total_result["count"] += result.new_count
                total_result["total_fetched"] += result.total_fetched
                total_result["duplicate_count"] += result.duplicate_count
                total_result["groups_collected"].append(group)
                
                if result.errors:
                    total_result["errors"].extend(result.errors)
                
                self.logger.info(
                    f"[{trace_id}] Group '{group}' done: "
                    f"fetched={result.total_fetched}, new={result.new_count}"
                )
                
            except Exception as e:
                self.logger.error(f"[{trace_id}] Group '{group}' failed: {e}")
                total_result["errors"].append(f"{group}: {str(e)}")
        
        self.logger.info(
            f"[{trace_id}] Collection done: "
            f"groups={total_result['groups_collected']}, "
            f"new={total_result['count']}"
        )
        
        return total_result
    
    async def collect_group(
        self,
        group: str,
        since_hours: int = 1,
        trace_id: Optional[str] = None,
    ) -> CollectResult:
        """手动采集指定分组 (忽略时间间隔)"""
        trace_id = trace_id or uuid.uuid4().hex[:8]
        result = await self._inner.collect_group(
            group=group,
            since_hours=since_hours,
            save_to_db=True,
            trace_id=trace_id,
        )
        # 更新上次采集时间 (持久化到 MongoDB)
        await self._save_collect_time(group, datetime.utcnow())
        return result
    
    async def collect_source(
        self,
        source_id: str,
        since_hours: int = 1,
        trace_id: Optional[str] = None,
    ) -> CollectResult:
        """手动采集指定源"""
        trace_id = trace_id or uuid.uuid4().hex[:8]
        since = datetime.utcnow() - timedelta(hours=since_hours)
        return await self._inner.collect_source(
            source_id=source_id,
            since=since,
            save_to_db=True,
            trace_id=trace_id,
        )
    
    async def collect_all(
        self,
        since_hours: int = 1,
        trace_id: Optional[str] = None,
    ) -> CollectResult:
        """手动采集所有源 (忽略时间间隔)"""
        trace_id = trace_id or uuid.uuid4().hex[:8]
        result = await self._inner.collect_all(
            since_hours=since_hours,
            limit_per_source=50,
            save_to_db=True,
            trace_id=trace_id,
        )
        # 更新所有分组的采集时间 (持久化到 MongoDB)
        now = datetime.utcnow()
        for group in GROUP_INTERVALS.keys():
            await self._save_collect_time(group, now)
        return result
    
    async def get_stats(self, trace_id: Optional[str] = None) -> Dict[str, Any]:
        """获取统计信息"""
        # 确保元数据已加载
        await self._load_metadata()
        
        stats = await self._inner.get_stats(trace_id=trace_id)
        stats["group_intervals"] = GROUP_INTERVALS
        stats["last_collect_time"] = {
            group: t.isoformat() if t else None
            for group, t in self._last_collect_time.items()
        }
        return stats
    
    @classmethod
    def get_available_sources(cls) -> List[Dict[str, str]]:
        """获取可用的采集源列表"""
        return _MultiSourceNewsCollector.get_available_sources()
    
    @classmethod
    def get_groups(cls) -> Dict[str, List[str]]:
        """获取所有分组"""
        return _MultiSourceNewsCollector.get_groups()
    
    @classmethod
    def get_group_intervals(cls) -> Dict[str, int]:
        """获取分组采集间隔"""
        return GROUP_INTERVALS.copy()


# 全局实例
multi_source_collector = MultiSourceCollector()
