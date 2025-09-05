#!/usr/bin/env python3

import asyncio
import json
import sys
from typing import Any, Dict, List, Optional, Union
import pymysql
from pymysql.cursors import DictCursor
import os
from dataclasses import dataclass

# MCP相关导入
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.server.stdio
import mcp.types as types

# 数据类定义
@dataclass
class ColumnInfo:
    columnName: str
    columnType: str
    isNullable: str
    columnComment: str
    columnDefault: Optional[str]
    extra: str

@dataclass
class TableInfo:
    tableName: str
    tableComment: str
    tableType: str
    engine: str
    tableRows: Optional[int]

@dataclass
class DatabaseResult:
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    message: str = ""

class DatabaseMCPServer:
    def __init__(self):
        self.server = Server("database-server")
        
        # 注册工具
        self._register_tools()
        
    def _register_tools(self):
        """注册所有数据库工具"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            return [
                types.Tool(
                    name="get_table_columns",
                    description="获取数据库表的列信息，包括列名、类型、是否可空、注释等详细信息",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "host": {
                                "type": "string",
                                "description": "数据库主机地址"
                            },
                            "user": {
                                "type": "string", 
                                "description": "数据库用户名"
                            },
                            "password": {
                                "type": "string",
                                "description": "数据库密码"
                            },
                            "database": {
                                "type": "string",
                                "description": "数据库名称"
                            },
                            "tableName": {
                                "type": "string",
                                "description": "表名"
                            },
                            "port": {
                                "type": "number",
                                "description": "数据库端口号，默认3306",
                                "default": 3306
                            }
                        },
                        "required": ["host", "user", "password", "database", "tableName"]
                    },
                ),
                types.Tool(
                    name="get_tables",
                    description="获取数据库中所有表的信息，包括表名、注释、类型、引擎等",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "host": {
                                "type": "string",
                                "description": "数据库主机地址"
                            },
                            "user": {
                                "type": "string",
                                "description": "数据库用户名"
                            },
                            "password": {
                                "type": "string",
                                "description": "数据库密码"
                            },
                            "database": {
                                "type": "string",
                                "description": "数据库名称"
                            },
                            "port": {
                                "type": "number",
                                "description": "数据库端口号，默认3306",
                                "default": 3306
                            }
                        },
                        "required": ["host", "user", "password", "database"]
                    },
                ),
                types.Tool(
                    name="execute_query",
                    description="执行自定义SQL查询语句",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "host": {
                                "type": "string",
                                "description": "数据库主机地址"
                            },
                            "user": {
                                "type": "string",
                                "description": "数据库用户名"
                            },
                            "password": {
                                "type": "string",
                                "description": "数据库密码"
                            },
                            "database": {
                                "type": "string",
                                "description": "数据库名称"
                            },
                            "query": {
                                "type": "string",
                                "description": "SQL查询语句"
                            },
                            "params": {
                                "type": "array",
                                "description": "查询参数（可选）",
                                "items": {"type": "string"}
                            },
                            "port": {
                                "type": "number",
                                "description": "数据库端口号，默认3306",
                                "default": 3306
                            }
                        },
                        "required": ["host", "user", "password", "database", "query"]
                    },
                ),
                types.Tool(
                    name="test_connection",
                    description="测试数据库连接是否正常",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "host": {
                                "type": "string",
                                "description": "数据库主机地址"
                            },
                            "user": {
                                "type": "string",
                                "description": "数据库用户名"
                            },
                            "password": {
                                "type": "string",
                                "description": "数据库密码"
                            },
                            "database": {
                                "type": "string",
                                "description": "数据库名称"
                            },
                            "port": {
                                "type": "number",
                                "description": "数据库端口号，默认3306",
                                "default": 3306
                            }
                        },
                        "required": ["host", "user", "password", "database"]
                    },
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: dict
        ) -> List[types.TextContent]:
            try:
                if name == "get_table_columns":
                    result = await self.get_table_columns(
                        arguments["host"],
                        arguments["user"], 
                        arguments["password"],
                        arguments["database"],
                        arguments["tableName"],
                        arguments.get("port", 3306)
                    )
                elif name == "get_tables":
                    result = await self.get_tables(
                        arguments["host"],
                        arguments["user"],
                        arguments["password"],
                        arguments["database"],
                        arguments.get("port", 3306)
                    )
                elif name == "execute_query":
                    result = await self.execute_query(
                        arguments["host"],
                        arguments["user"],
                        arguments["password"],
                        arguments["database"],
                        arguments["query"],
                        arguments.get("params"),
                        arguments.get("port", 3306)
                    )
                elif name == "test_connection":
                    result = await self.test_connection(
                        arguments["host"],
                        arguments["user"],
                        arguments["password"],
                        arguments["database"],
                        arguments.get("port", 3306)
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

    def create_connection(self, host: str, user: str, password: str, database: str, port: int = 3306):
        """创建数据库连接"""
        try:
            connection = pymysql.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port,
                charset='utf8mb4',
                cursorclass=DictCursor
            )
            return connection
        except Exception as e:
            raise Exception(f"Failed to connect to database: {str(e)}")

    async def get_table_columns(
        self, host: str, user: str, password: str, database: str, 
        table_name: str, port: int = 3306
    ) -> DatabaseResult:
        """获取表列信息"""
        print(f"🔍 Getting columns for table: {table_name} in database: {database}")
        
        connection = None
        try:
            connection = self.create_connection(host, user, password, database, port)
            
            with connection.cursor() as cursor:
                sql = """
                    SELECT 
                        column_name as columnName,
                        column_type as columnType,
                        is_nullable as isNullable,
                        IFNULL(column_comment, '') as columnComment,
                        column_default as columnDefault,
                        extra as extra
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """
                
                cursor.execute(sql, (database, table_name))
                rows = cursor.fetchall()
                
                columns = [ColumnInfo(**row) for row in rows]
                
                if columns:
                    print(f"   - Found {len(columns)} columns in table {table_name}")
                    return DatabaseResult(
                        success=True,
                        data=[col.__dict__ for col in columns],
                        message=f"Successfully retrieved {len(columns)} columns from table {table_name}"
                    )
                else:
                    return DatabaseResult(
                        success=False,
                        message=f"Table {table_name} does not exist or has no columns in database {database}"
                    )
                    
        except Exception as e:
            print(f"Error getting table columns: {e}")
            return DatabaseResult(
                success=False,
                error=str(e),
                message=f"Failed to get columns for table {table_name}: {str(e)}"
            )
        finally:
            if connection:
                connection.close()

    async def get_tables(
        self, host: str, user: str, password: str, database: str, port: int = 3306
    ) -> DatabaseResult:
        """获取数据库中所有表"""
        print(f"📊 Getting tables from database: {database}")
        
        connection = None
        try:
            connection = self.create_connection(host, user, password, database, port)
            
            with connection.cursor() as cursor:
                sql = """
                    SELECT 
                        table_name as tableName,
                        IFNULL(table_comment, '') as tableComment,
                        table_type as tableType,
                        IFNULL(engine, '') as engine,
                        table_rows as tableRows
                    FROM information_schema.tables 
                    WHERE table_schema = %s
                    ORDER BY table_name
                """
                
                cursor.execute(sql, (database,))
                rows = cursor.fetchall()
                
                tables = [TableInfo(**row) for row in rows]
                
                print(f"   - Found {len(tables)} tables in database {database}")
                return DatabaseResult(
                    success=True,
                    data=[table.__dict__ for table in tables],
                    message=f"Successfully retrieved {len(tables)} tables from database {database}"
                )
                
        except Exception as e:
            print(f"Error getting tables: {e}")
            return DatabaseResult(
                success=False,
                error=str(e),
                message=f"Failed to get tables from database {database}: {str(e)}"
            )
        finally:
            if connection:
                connection.close()

    async def execute_query(
        self, host: str, user: str, password: str, database: str, 
        query: str, params: Optional[List[str]] = None, port: int = 3306
    ) -> DatabaseResult:
        """执行自定义SQL查询"""
        print(f"⚡ Executing query: {query[:100]}...")
        
        connection = None
        try:
            connection = self.create_connection(host, user, password, database, port)
            
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                
                # 如果是SELECT查询，获取结果
                if query.strip().upper().startswith('SELECT'):
                    rows = cursor.fetchall()
                    return DatabaseResult(
                        success=True,
                        data=rows,
                        message="Query executed successfully"
                    )
                else:
                    # 对于INSERT, UPDATE, DELETE等操作
                    connection.commit()
                    return DatabaseResult(
                        success=True,
                        data={"affected_rows": cursor.rowcount},
                        message=f"Query executed successfully. Affected rows: {cursor.rowcount}"
                    )
                
        except Exception as e:
            print(f"Error executing query: {e}")
            return DatabaseResult(
                success=False,
                error=str(e),
                message=f"Failed to execute query: {str(e)}"
            )
        finally:
            if connection:
                connection.close()

    async def test_connection(
        self, host: str, user: str, password: str, database: str, port: int = 3306
    ) -> DatabaseResult:
        """测试数据库连接"""
        print(f"🔌 Testing connection to database: {database}@{host}")
        
        connection = None
        try:
            connection = self.create_connection(host, user, password, database, port)
            
            with connection.cursor() as cursor:
                cursor.execute('SELECT 1')
                
            return DatabaseResult(
                success=True,
                message=f"Successfully connected to database {database}@{host}"
            )
            
        except Exception as e:
            print(f"Connection test failed: {e}")
            return DatabaseResult(
                success=False,
                error=str(e),
                message=f"Failed to connect to database {database}@{host}: {str(e)}"
            )
        finally:
            if connection:
                connection.close()

    async def run(self):
        """运行MCP服务器"""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="database-server",
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
    
    server = DatabaseMCPServer()
    print("Database MCP Server running on stdio", file=sys.stderr)
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nShutting down server...", file=sys.stderr)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()