"""
工具基类

所有 MCP 工具必须继承此类。
"""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, Type
import time
import logging

from pydantic import BaseModel


InputT = TypeVar("InputT", bound=BaseModel)
OutputT = TypeVar("OutputT", bound=BaseModel)


class ToolResult(BaseModel):
    """工具执行结果"""
    success: bool = True
    error_message: Optional[str] = None
    execution_time_ms: float = 0


class BaseTool(ABC, Generic[InputT, OutputT]):
    """
    工具基类
    
    所有 MCP 工具必须继承此类。
    
    Example:
        class GetStockBasicInput(BaseModel):
            ts_code: str
        
        class GetStockBasicOutput(ToolResult):
            data: dict
        
        class GetStockBasicTool(BaseTool[GetStockBasicInput, GetStockBasicOutput]):
            name = "get_stock_basic"
            description = "获取股票基础信息"
            input_model = GetStockBasicInput
            output_model = GetStockBasicOutput
            
            async def execute(self, input: GetStockBasicInput) -> GetStockBasicOutput:
                data = await tushare_manager.get_stock_basic(input.ts_code)
                return GetStockBasicOutput(data=data)
    """
    
    name: str
    description: str
    input_model: Type[InputT]
    output_model: Type[OutputT]
    
    def __init__(self):
        self.logger = logging.getLogger(f"tool.{self.name}")
    
    @abstractmethod
    async def execute(self, input_data: InputT) -> OutputT:
        """
        执行工具
        
        子类必须实现此方法。
        """
        raise NotImplementedError
    
    async def __call__(self, input_data: InputT) -> OutputT:
        """调用工具"""
        start_time = time.time()
        
        try:
            result = await self.execute(input_data)
            result.execution_time_ms = (time.time() - start_time) * 1000
            return result
        except Exception as e:
            self.logger.exception(f"Tool execution failed: {e}")
            return self.output_model(
                success=False,
                error_message=str(e),
                execution_time_ms=(time.time() - start_time) * 1000,
            )
    
    def get_schema(self) -> dict:
        """获取工具 Schema (用于 LLM Function Calling)"""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.input_model.model_json_schema(),
        }
