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
class RuleGenerateResult:
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    message: str = ""

class RuleGenerateMCPServer:
    def __init__(self, api_base_url: str = "http://localhost:8787"):
        self.server = Server("rule-generate-server")
        self.api_base_url = api_base_url.rstrip('/')
        self.api_endpoint = f"{self.api_base_url}/api/v1/rule-generator/generate-rules"
        
        # 注册工具
        self._register_tools()
        
    def _register_tools(self):
        """注册规则生成工具"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            return [
                types.Tool(
                    name="generate_rules",
                    description="根据表结构、评估指标和数据库配置生成数据质量评估规则集",
                    inputSchema={
                        "type": "object",
                        "properties": {
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
                            "assessment_indicators": {
                                "type": "object",
                                "description": "评估指标信息",
                                "properties": {
                                    "indicators": {
                                        "type": "array",
                                        "description": "指标列表",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "dimension_name": {"type": "string", "description": "维度名称"},
                                                "indicator_name": {"type": "string", "description": "指标名称"},
                                                "indicator_definition": {"type": "string", "description": "指标定义"},
                                                "indicator_mode": {"type": "number", "description": "指标评估模式，单表并发为0，多表串行为1"}
                                            },
                                            "required": ["dimension_name", "indicator_name", "indicator_definition", "indicator_mode"]
                                        }
                                    }
                                },
                                "required": ["indicators"]
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
                            "use_sandbox": {
                                "type": "boolean",
                                "description": "是否使用沙盒模式",
                                "default": True
                            }
                        },
                        "required": ["table_schema", "assessment_indicators", "database_config"]
                    },
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: dict
        ) -> List[types.TextContent]:
            try:
                if name == "generate_rules":
                    result = await self.generate_rules(
                        table_schema=arguments["table_schema"],
                        assessment_indicators=arguments["assessment_indicators"],
                        database_config=arguments["database_config"],
                        use_sandbox=arguments.get("use_sandbox", True)
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

    async def generate_rules(
        self, 
        table_schema: Dict[str, Any], 
        assessment_indicators: Dict[str, Any],
        database_config: Dict[str, Any],
        use_sandbox: bool = True
    ) -> RuleGenerateResult:
        """调用规则生成API"""
        tables = table_schema.get('tables', [])
        table_names = [table.get('table_name', 'unknown') for table in tables]
        print(f"🔄 Generating rules for tables: {', '.join(table_names)}")
        
        try:
            # 构建请求数据
            request_data = {
                "table_schema": table_schema,
                "assessment_indicators": assessment_indicators,
                "database_config": database_config,
                "use_sandbox": use_sandbox
            }
            
            print(f"   📊 Tables count: {len(tables)}")
            print(f"   📏 Indicators count: {len(assessment_indicators.get('indicators', []))}")
            print(f"   🔒 Sandbox mode: {use_sandbox}")
            
            # 发送POST请求
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_endpoint,
                    json=request_data,
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=300)  # 5分钟超时
                ) as response:
                    response_text = await response.text()
                    
                    if response.status == 200:
                        result_data = json.loads(response_text)
                        print(f"   ✅ Successfully generated rules for {len(tables)} table(s)")
                        return RuleGenerateResult(
                            success=True,
                            data=result_data,
                            message=f"Rules generated successfully for tables: {', '.join(table_names)}"
                        )
                    else:
                        print(f"   ❌ API request failed with status {response.status}")
                        error_detail = response_text
                        try:
                            error_json = json.loads(response_text)
                            error_detail = error_json.get('detail', response_text)
                        except:
                            pass
                        
                        return RuleGenerateResult(
                            success=False,
                            error=f"HTTP {response.status}",
                            message=f"API request failed: {error_detail}"
                        )
                        
        except aiohttp.ClientTimeout:
            print("   ⏱️ Request timeout")
            return RuleGenerateResult(
                success=False,
                error="Request timeout",
                message="Rule generation request timed out after 5 minutes"
            )
        except json.JSONDecodeError as e:
            print(f"   ❌ JSON decode error: {e}")
            return RuleGenerateResult(
                success=False,
                error="Invalid JSON response",
                message=f"Failed to decode API response: {str(e)}"
            )
        except Exception as e:
            print(f"   ❌ Error generating rules: {e}")
            return RuleGenerateResult(
                success=False,
                error=str(e),
                message=f"Failed to generate rules: {str(e)}"
            )

    async def run(self):
        """运行MCP服务器"""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="rule-generate-server",
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
    api_base_url = os.getenv("RULE_GENERATE_API_URL", "http://localhost:8787")
    
    server = RuleGenerateMCPServer(api_base_url)
    print(f"Rule Generate MCP Server running on stdio (API: {api_base_url})", file=sys.stderr)
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nShutting down server...", file=sys.stderr)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()