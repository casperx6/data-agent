#!/usr/bin/env python3

import asyncio
import json
import sys
from typing import Any, Dict, List, Optional, Union
import aiohttp
import os
from dataclasses import dataclass

# MCP相关导入
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.server.stdio
import mcp.types as types

# 数据类定义
@dataclass
class InvalidDataGetResult:
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    message: str = ""

class InvalidDataGetMCPServer:
    def __init__(self, api_base_url: str = "http://localhost:8787"):
        self.server = Server("invalid-data-get-server")
        self.api_base_url = api_base_url.rstrip('/')
        self.api_endpoint = f"{self.api_base_url}/api/v1/invalid-data-getter/get-invalid-data"
        
        # 注册工具
        self._register_tools()
        
    def _register_tools(self):
        """注册无效数据获取工具"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            return [
                types.Tool(
                    name="get_invalid_data",
                    description="根据数据质量评估规则获取无效数据记录",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "rule_detail": {
                                "type": "object",
                                "description": "规则详情信息",
                                "properties": {
                                    "assessment_dimension": {"type": "string", "description": "评估维度"},
                                    "assessment_indicator": {"type": "string", "description": "评估指标"},
                                    "assessment_object": {"type": "string", "description": "评估对象"},
                                    "assessment_content": {"type": "string", "description": "评估内容描述"},
                                    "assessment_sql": {"type": "string", "description": "评估SQL语句"},
                                    "exception_count": {"type": "number", "description": "异常数量"},
                                    "execution_status": {"type": "string", "description": "执行状态"},
                                    "passed": {"type": "boolean", "description": "是否通过评估"}
                                },
                                "required": ["assessment_dimension", "assessment_indicator", "assessment_object", "assessment_content", "assessment_sql"]
                            },
                            "table_schema": {
                                "type": "object",
                                "description": "表结构信息",
                                "properties": {
                                    "tables": {
                                        "type": "array",
                                        "description": "数据库表列表",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "table_name": {"type": "string", "description": "表名"},
                                                "table_comment": {"type": "string", "description": "表注释"},
                                                "columns": {
                                                    "type": "array",
                                                    "description": "表列信息",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "name": {"type": "string", "description": "列名"},
                                                            "type": {"type": "string", "description": "列类型"},
                                                            "description": {"type": "string", "description": "列描述"},
                                                            "nullable": {"type": "boolean", "description": "是否可空"},
                                                            "primary_key": {"type": "boolean", "description": "是否主键"},
                                                            "foreign_key": {
                                                                "type": "object",
                                                                "description": "外键信息",
                                                                "properties": {
                                                                    "reference_table": {"type": "string"},
                                                                    "reference_field": {"type": "string"}
                                                                }
                                                            }
                                                        },
                                                        "required": ["name", "type"]
                                                    }
                                                }
                                            },
                                            "required": ["table_name", "columns"]
                                        }
                                    }
                                },
                                "required": ["tables"]
                            },
                            "database_config": {
                                "type": "object",
                                "description": "数据库连接配置",
                                "properties": {
                                    "host": {"type": "string", "description": "数据库主机地址"},
                                    "port": {"type": "number", "description": "数据库端口号"},
                                    "user": {"type": "string", "description": "数据库用户名"},
                                    "password": {"type": "string", "description": "数据库密码"},
                                    "database": {"type": "string", "description": "数据库名称"}
                                },
                                "required": ["host", "port", "user", "password", "database"]
                            },
                            "database_type": {
                                "type": "string",
                                "description": "数据库类型",
                                "default": "mysql"
                            },
                            # "limit": {
                            #     "type": "number",
                            #     "description": "返回记录数限制",
                            #     "default": 100
                            # }
                        },
                        "required": ["rule_detail", "table_schema", "database_config"]
                    },
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: dict
        ) -> List[types.TextContent]:
            try:
                if name == "get_invalid_data":
                    result = await self.get_invalid_data(
                        rule_detail=arguments["rule_detail"],
                        table_schema=arguments["table_schema"],
                        database_config=arguments["database_config"],
                        database_type=arguments.get("database_type", "mysql"),
                        # limit=arguments.get("limit", 100)
                    )
                else:
                    raise ValueError(f"Unknown tool: {name}")
                
                return [types.TextContent(
                    type="text",
                    text=json.dumps(result.__dict__ if hasattr(result, '__dict__') else result, 
                                  ensure_ascii=False, indent=2, default=str)
                )]
                
            except Exception as e:
                return [types.TextContent(
                    type="text", 
                    text=f"Error: {str(e)}"
                )]

    async def get_invalid_data(
        self, 
        rule_detail: Dict[str, Any],
        table_schema: Dict[str, Any], 
        database_config: Dict[str, Any],
        database_type: str = "mysql",
        # limit: int = 100
    ) -> InvalidDataGetResult:
        """调用无效数据获取API"""
        assessment_object = rule_detail.get('assessment_object', 'unknown')
        assessment_indicator = rule_detail.get('assessment_indicator', 'unknown')
        exception_count = rule_detail.get('exception_count', 0)
        
        print(f"🔍 Getting invalid data for: {assessment_object}")
        print(f"   📊 Assessment indicator: {assessment_indicator}")
        print(f"   ❌ Exception count: {exception_count}")
        # print(f"   📝 Limit: {limit}")
        
        try:
            # 构建请求数据
            request_data = {
                "rule_detail": rule_detail,
                "table_schema": table_schema,
                "database_config": database_config,
                "database_type": database_type,
                # "limit": limit
            }
            
            # 发送POST请求
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_endpoint,
                    json=request_data,
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=180)  # 3分钟超时
                ) as response:
                    response_text = await response.text()
                    
                    if response.status == 200:
                        result_data = json.loads(response_text)
                        print(f"   ✅ Successfully retrieved invalid data")
                        return InvalidDataGetResult(
                            success=True,
                            data=result_data,
                            message=f"Invalid data retrieved successfully for {assessment_object}"
                        )
                    else:
                        print(f"   ❌ API request failed with status {response.status}")
                        error_detail = response_text
                        try:
                            error_json = json.loads(response_text)
                            error_detail = error_json.get('detail', response_text)
                        except:
                            pass
                        
                        return InvalidDataGetResult(
                            success=False,
                            error=f"HTTP {response.status}",
                            message=f"API request failed: {error_detail}"
                        )
                        
        except aiohttp.ClientTimeout:
            print("   ⏱️ Request timeout")
            return InvalidDataGetResult(
                success=False,
                error="Request timeout",
                message="Invalid data retrieval request timed out after 3 minutes"
            )
        except json.JSONDecodeError as e:
            print(f"   ❌ JSON decode error: {e}")
            return InvalidDataGetResult(
                success=False,
                error="Invalid JSON response",
                message=f"Failed to decode API response: {str(e)}"
            )
        except Exception as e:
            print(f"   ❌ Error getting invalid data: {e}")
            return InvalidDataGetResult(
                success=False,
                error=str(e),
                message=f"Failed to get invalid data: {str(e)}"
            )

    async def run(self):
        """运行MCP服务器"""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="invalid-data-get-server",
                    server_version="0.1.0",
                    capabilities=self.server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )

def main():
    """主函数"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # 从环境变量获取API地址
    api_base_url = os.getenv("INVALID_DATA_GET_API_URL", "http://localhost:8787")
    
    server = InvalidDataGetMCPServer(api_base_url)
    print(f"Invalid Data Get MCP Server running on stdio (API: {api_base_url})", file=sys.stderr)
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nShutting down server...", file=sys.stderr)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()