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
class ReportGenerateResult:
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    message: str = ""

class ReportGenerateMCPServer:
    def __init__(self, api_base_url: str = "http://localhost:8787"):
        self.server = Server("report-generate-server")
        self.api_base_url = api_base_url.rstrip('/')
        self.api_endpoint = f"{self.api_base_url}/api/v1/report-generator/generate-report"
        
        # 注册工具
        self._register_tools()
        
    def _register_tools(self):
        """注册报告生成工具"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            return [
                types.Tool(
                    name="generate_report",
                    description="根据表结构、评估指标和评估结果生成数据质量评估报告",
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
                                                "indicator_definition": {"type": "string", "description": "指标定义"}
                                            },
                                            "required": ["dimension_name", "indicator_name", "indicator_definition"]
                                        }
                                    }
                                },
                                "required": ["indicators"]
                            },
                            "assessment_results": {
                                "type": "object",
                                "description": "评估结果信息",
                                "properties": {
                                    "execution_id": {
                                    "type": "string",
                                    "description": "执行ID"
                                    },
                                    "summary": {
                                    "type": "object",
                                    "description": "评估结果摘要",
                                    "properties": {
                                        "total_rules": {
                                        "type": "number",
                                        "description": "总规则数"
                                        },
                                        "passed_rules": {
                                        "type": "number",
                                        "description": "通过规则数"
                                        },
                                        "failed_rules": {
                                        "type": "number",
                                        "description": "失败规则数"
                                        },
                                        "error_rules": {
                                        "type": "number",
                                        "description": "错误规则数"
                                        },
                                        "total_exception_count": {
                                        "type": "number",
                                        "description": "总异常数"
                                        },
                                        "success_rate": {
                                        "type": "number",
                                        "description": "成功率"
                                        },
                                        "dimension_success_rate": {
                                        "type": "object",
                                        "description": "各维度成功率"
                                        }
                                    }
                                    },
                                    "rule_results": {
                                    "type": "array",
                                    "description": "规则结果列表",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                        "assessment_dimension": { "type": "string", "description": "评估维度" },
                                        "assessment_indicator": { "type": "string", "description": "评估指标" },
                                        "assessment_object": { "type": "string", "description": "评估对象" },
                                        "assessment_content": { "type": "string", "description": "评估内容" },
                                        "assessment_sql": { "type": "string", "description": "评估SQL" },
                                        "execution_status": { "type": "string", "description": "执行状态" },
                                        "passed": { "type": "boolean", "description": "是否通过" },
                                        "exception_count": { "type": "number", "description": "异常数量" },
                                        "execution_time_ms": { "type": "number", "description": "执行时间(毫秒)" },
                                        "error_message": { "type": ["string", "null"], "description": "错误消息" }
                                        }
                                    }
                                    },
                                    "database_info": {
                                    "type": "object",
                                    "description": "数据库信息"
                                    },
                                    "total_execution_time_ms": {
                                    "type": "number",
                                    "description": "总执行时间(毫秒)"
                                    }
                                },
                                "required": ["summary", "rule_results"]
                                }
                        #     "output_path": {
                        #         "type": "string",
                        #         "description": "报告输出路径",
                        #         "default": "./reports/"
                        #     },
                        #     "report_id": {
                        #         "type": "string",
                        #         "description": "报告ID",
                        #         "default": "auto_generated"
                        #     },
                        #     "report_format": {
                        #         "type": "string",
                        #         "description": "报告格式",
                        #         "enum": ["html", "pdf", "json", "excel"],
                        #         "default": "html"
                        #     },
                        #     "include_charts": {
                        #         "type": "boolean",
                        #         "description": "是否包含图表",
                        #         "default": True
                        #     },
                        #     "language": {
                        #         "type": "string",
                        #         "description": "报告语言",
                        #         "enum": ["zh", "en"],
                        #         "default": "zh"
                        #     }
                        },
                        "required": ["table_schema", "assessment_indicators", "assessment_results"]
                    },
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: dict
        ) -> List[types.TextContent]:
            try:
                if name == "generate_report":
                    result = await self.generate_report(
                        table_schema=arguments["table_schema"],
                        assessment_indicators=arguments["assessment_indicators"],
                        assessment_results=arguments["assessment_results"],
                        # output_path=arguments.get("output_path", "./reports/"),
                        # report_id=arguments.get("report_id", "auto_generated"),
                        # report_format=arguments.get("report_format", "html"),
                        # include_charts=arguments.get("include_charts", True),
                        # language=arguments.get("language", "zh")
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

    async def generate_report(
        self, 
        table_schema: Dict[str, Any],
        assessment_indicators: Dict[str, Any],
        assessment_results: Dict[str, Any],
        # output_path: str = "./reports/",
        # report_id: str = "auto_generated",
        # report_format: str = "html",
        # include_charts: bool = True,
        # language: str = "zh"
    ) -> ReportGenerateResult:
        """调用报告生成API"""
        execution_id = assessment_results.get('execution_id', 'unknown')
        summary = assessment_results.get('summary', {}).get('summary', {})
        total_rules = summary.get('total_rules', 0)
        passed_rules = summary.get('passed_rules', 0)
        success_rate = summary.get('success_rate', 0)
        
        print(f"📊 Generating report for execution: {execution_id}")
        # print(f"   📝 Report ID: {report_id}")
        # print(f"   📄 Format: {report_format}")
        # print(f"   📈 Charts included: {include_charts}")
        # print(f"   🌐 Language: {language}")
        print(f"   📋 Rules: {passed_rules}/{total_rules} passed ({success_rate:.1%})")
        
        try:
            # 构建请求数据
            request_data = {
                "table_schema": table_schema,
                "assessment_indicators": assessment_indicators,
                "assessment_results": assessment_results,
                # "output_path": output_path,
                # "report_id": report_id,
                # "report_format": report_format,
                # "include_charts": include_charts,
                # "language": language
            }
            
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
                        # print(f"   ✅ Successfully generated {report_format} report")
                        if result_data.get('report_path'):
                            print(f"   📁 Report saved to: {result_data['report_path']}")
                        return ReportGenerateResult(
                            success=True,
                            data=result_data,
                            message=f"Report generated successfully:)"
                        )
                    else:
                        print(f"   ❌ API request failed with status {response.status}")
                        error_detail = response_text
                        try:
                            error_json = json.loads(response_text)
                            error_detail = error_json.get('detail', response_text)
                        except:
                            pass
                        
                        return ReportGenerateResult(
                            success=False,
                            error=f"HTTP {response.status}",
                            message=f"API request failed: {error_detail}"
                        )
                        
        except aiohttp.ClientTimeout:
            print("   ⏱️ Request timeout")
            return ReportGenerateResult(
                success=False,
                error="Request timeout",
                message="Report generation request timed out after 5 minutes"
            )
        except json.JSONDecodeError as e:
            print(f"   ❌ JSON decode error: {e}")
            return ReportGenerateResult(
                success=False,
                error="Invalid JSON response",
                message=f"Failed to decode API response: {str(e)}"
            )
        except Exception as e:
            print(f"   ❌ Error generating report: {e}")
            return ReportGenerateResult(
                success=False,
                error=str(e),
                message=f"Failed to generate report: {str(e)}"
            )

    async def run(self):
        """运行MCP服务器"""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="report-generate-server",
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
    api_base_url = os.getenv("REPORT_GENERATE_API_URL", "http://localhost:8787")
    
    server = ReportGenerateMCPServer(api_base_url)
    print(f"Report Generate MCP Server running on stdio (API: {api_base_url})", file=sys.stderr)
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nShutting down server...", file=sys.stderr)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()