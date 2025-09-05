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
class RuleExecuteResult:
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    message: str = ""

class RuleExecuteMCPServer:
    def __init__(self, api_base_url: str = "http://localhost:8787"):
        self.server = Server("rule-execute-server")
        self.api_base_url = api_base_url.rstrip('/')
        self.api_endpoint = f"{self.api_base_url}/api/v1/rule-executor/execute-rules"
        
        # 注册工具
        self._register_tools()
        
    def _register_tools(self):
        """注册规则执行工具"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            return [
                types.Tool(
                    name="execute_rules",
                    description="执行数据质量评估规则集，对指定数据库运行SQL查询并返回评估结果",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "rule_set": {
                                "type": "object",
                                "description": "规则集信息",
                                "properties": {
                                    "rules": {
                                        "type": "array",
                                        "description": "规则列表",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "assessment_dimension": {"type": "string", "description": "评估维度"},
                                                "assessment_indicator": {"type": "string", "description": "评估指标"},
                                                "assessment_object": {"type": "string", "description": "评估对象"},
                                                "assessment_content": {"type": "string", "description": "评估内容描述"},
                                                "assessment_sql": {"type": "string", "description": "评估SQL语句"},
                                                "sql_status": {"type": "string", "description": "SQL状态"}
                                            },
                                            "required": ["assessment_dimension", "assessment_indicator", "assessment_object", "assessment_content", "assessment_sql"]
                                        }
                                    }
                                },
                                "required": ["rules"]
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
                                "enum": ["mysql", "postgresql", "sqlite", "oracle", "sqlserver"],
                                "default": "mysql"
                            },
                            # "parallel_execution": {
                            #     "type": "boolean",
                            #     "description": "是否并行执行规则",
                            #     "default": True
                            # },
                            # "timeout": {
                            #     "type": "number",
                            #     "description": "单个规则执行超时时间（秒）",
                            #     "default": 30
                            # }
                        },
                        "required": ["rule_set", "database_config"]
                    },
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: dict
        ) -> List[types.TextContent]:
            try:
                if name == "execute_rules":
                    result = await self.execute_rules(
                        rule_set=arguments["rule_set"],
                        database_config=arguments["database_config"],
                        database_type=arguments.get("database_type", "mysql"),
                        # parallel_execution=arguments.get("parallel_execution", True),
                        # timeout=arguments.get("timeout", 30)
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

    async def execute_rules(
        self, 
        rule_set: Dict[str, Any], 
        database_config: Dict[str, Any],
        database_type: str = "mysql",
        # parallel_execution: bool = True,
        # timeout: int = 30
    ) -> RuleExecuteResult:
        """调用规则执行API"""
        rules = rule_set.get('rules', [])
        print(f"🚀 Executing {len(rules)} data quality assessment rules")
        
        try:
            # 构建请求数据
            request_data = {
                "rule_set": rule_set,
                "database_config": database_config,
                "database_type": database_type
            }
            
            # # 添加可选参数
            # if not parallel_execution:
            #     request_data["parallel_execution"] = parallel_execution
            # if timeout != 30:
            #     request_data["timeout"] = timeout
            
            print(f"   📊 Rules count: {len(rules)}")
            print(f"   🗄️ Database: {database_config.get('database', 'unknown')} ({database_type})")
            print(f"   🔗 Host: {database_config.get('host', 'unknown')}:{database_config.get('port', 'unknown')}")
            # print(f"   ⚡ Parallel execution: {parallel_execution}")
            # print(f"   ⏱️ Timeout per rule: {timeout}s")
            
            # 发送POST请求
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_endpoint,
                    json=request_data,
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=600)  # 10分钟总超时
                ) as response:
                    response_text = await response.text()
                    
                    if response.status == 200:
                        result_data = json.loads(response_text)
                        
                        # 统计执行结果
                        if isinstance(result_data, dict) and 'results' in result_data:
                            results = result_data.get('results', [])
                            success_count = sum(1 for r in results if r.get('status') == 'success')
                            error_count = len(results) - success_count
                            
                            print(f"   ✅ Successfully executed {success_count}/{len(results)} rules")
                            if error_count > 0:
                                print(f"   ❌ {error_count} rules failed")
                        else:
                            print(f"   ✅ Rules execution completed")
                        
                        return RuleExecuteResult(
                            success=True,
                            data=result_data,
                            message=f"Successfully executed {len(rules)} rules"
                        )
                    else:
                        print(f"   ❌ API request failed with status {response.status}")
                        error_detail = response_text
                        try:
                            error_json = json.loads(response_text)
                            error_detail = error_json.get('detail', response_text)
                        except:
                            pass
                        
                        return RuleExecuteResult(
                            success=False,
                            error=f"HTTP {response.status}",
                            message=f"API request failed: {error_detail}"
                        )
                        
        except aiohttp.ClientTimeout:
            print("   ⏱️ Request timeout")
            return RuleExecuteResult(
                success=False,
                error="Request timeout",
                message="Rule execution request timed out after 10 minutes"
            )
        except json.JSONDecodeError as e:
            print(f"   ❌ JSON decode error: {e}")
            return RuleExecuteResult(
                success=False,
                error="Invalid JSON response",
                message=f"Failed to decode API response: {str(e)}"
            )
        except Exception as e:
            print(f"   ❌ Error executing rules: {e}")
            return RuleExecuteResult(
                success=False,
                error=str(e),
                message=f"Failed to execute rules: {str(e)}"
            )

    async def run(self):
        """运行MCP服务器"""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="rule-execute-server",
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
    api_base_url = os.getenv("RULE_EXECUTE_API_URL", "http://localhost:8787")
    
    server = RuleExecuteMCPServer(api_base_url)
    print(f"Rule Execute MCP Server running on stdio (API: {api_base_url})", file=sys.stderr)
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nShutting down server...", file=sys.stderr)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()