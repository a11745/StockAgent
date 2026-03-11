"""
报告回顾 API

提供报告查询和分页接口。
- 支持按日期查询单日报告
- 支持分页查询全部报告
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
from datetime import datetime

from core.managers import mongo_manager

router = APIRouter()


@router.get("")
async def list_reports(
    date: Optional[str] = Query(default=None, description="日期筛选 (YYYY-MM-DD)"),
    report_type: Optional[str] = Query(default=None, description="报告类型 (morning/noon)"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=10, ge=1, le=50, description="每页条数"),
) -> Dict[str, Any]:
    """
    获取报告列表
    
    Args:
        date: 日期筛选 (YYYY-MM-DD 格式)，不传则返回全部
        report_type: 报告类型筛选 (morning/noon)
        page: 页码
        page_size: 每页条数
    
    Returns:
        分页的报告列表
    """
    query: Dict[str, Any] = {}
    
    if date:
        query["date"] = date
    
    if report_type:
        if report_type not in ("morning", "noon"):
            raise HTTPException(status_code=400, detail="report_type must be 'morning' or 'noon'")
        query["type"] = report_type
    
    total = await mongo_manager.count("reports", query)
    
    skip = (page - 1) * page_size
    docs = await mongo_manager.find_many(
        "reports",
        query,
        sort=[("created_at", -1)],
        skip=skip,
        limit=page_size,
        projection={
            "_id": 1,
            "type": 1,
            "date": 1,
            "title": 1,
            "overview": 1,
            "stats": 1,
            "created_at": 1,
            "pushed": 1,
        },
    )
    
    items = []
    for doc in docs:
        items.append({
            "id": doc.get("_id"),
            "type": doc.get("type"),
            "date": doc.get("date"),
            "title": doc.get("title"),
            "overview": doc.get("overview", ""),
            "stats": doc.get("stats", {}),
            "created_at": doc.get("created_at"),
            "pushed": doc.get("pushed", {}),
        })
    
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


@router.get("/dates")
async def get_report_dates(
    limit: int = Query(default=30, ge=1, le=100, description="返回天数"),
) -> Dict[str, Any]:
    """
    获取有报告的日期列表
    
    用于前端日期选择器显示可选日期
    
    Args:
        limit: 返回天数上限
    
    Returns:
        日期列表
    """
    docs = await mongo_manager.find_many(
        "reports",
        {},
        sort=[("date", -1)],
        projection={"date": 1, "_id": 0},
    )
    
    unique_dates = list(dict.fromkeys(d.get("date") for d in docs if d.get("date")))
    
    return {
        "dates": unique_dates[:limit],
    }


@router.get("/{report_id}")
async def get_report_detail(report_id: str) -> Dict[str, Any]:
    """
    获取报告详情
    
    Args:
        report_id: 报告ID (如 morning_20260310)
    
    Returns:
        完整的报告内容
    """
    doc = await mongo_manager.find_one("reports", {"_id": report_id})
    
    if not doc:
        raise HTTPException(status_code=404, detail="Report not found")
    
    sections = doc.get("sections", [])
    formatted_sections = []
    for section in sections:
        formatted_sections.append({
            "category": section.get("category"),
            "title": section.get("title"),
            "summary": section.get("summary", ""),
            "item_count": section.get("item_count", len(section.get("items", []))),
            "items": [
                {
                    "event_id": item.get("event_id"),
                    "title": item.get("title"),
                    "summary": item.get("summary", ""),
                    "importance": item.get("importance"),
                    "news_count": item.get("news_count", 1),
                    "ts_codes": item.get("ts_codes", []),
                    "event_time": item.get("event_time"),
                }
                for item in section.get("items", [])
            ],
        })
    
    return {
        "id": doc.get("_id"),
        "type": doc.get("type"),
        "date": doc.get("date"),
        "title": doc.get("title"),
        "overview": doc.get("overview", ""),
        "sections": formatted_sections,
        "content_markdown": doc.get("content_markdown", ""),
        "content_wechat": doc.get("content_wechat", ""),
        "stats": doc.get("stats", {}),
        "created_at": doc.get("created_at"),
        "pushed": doc.get("pushed", {}),
    }
