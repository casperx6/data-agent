#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

// 导入你的数据库函数
import { 
  getTableColumns, 
  getTables, 
  executeQuery, 
  testConnection 
} from "./get-table-columns.js";

// 定义参数类型接口
interface DatabaseConnectionArgs {
  host: string;
  user: string;
  password: string;
  database: string;
  port?: number;
}

interface GetTableColumnsArgs extends DatabaseConnectionArgs {
  tableName: string;
}

interface ExecuteQueryArgs extends DatabaseConnectionArgs {
  query: string;
  params?: any[];
}

// 类型守卫函数 - 注意参数类型改为 any
function isDatabaseConnectionArgs(args: any): args is DatabaseConnectionArgs {
  return args && 
         typeof args.host === 'string' &&
         typeof args.user === 'string' &&
         typeof args.password === 'string' &&
         typeof args.database === 'string' &&
         (args.port === undefined || typeof args.port === 'number');
}

function isGetTableColumnsArgs(args: any): args is GetTableColumnsArgs {
  return args &&
         typeof args.host === 'string' &&
         typeof args.user === 'string' &&
         typeof args.password === 'string' &&
         typeof args.database === 'string' &&
         typeof args.tableName === 'string' &&
         (args.port === undefined || typeof args.port === 'number');
}

function isExecuteQueryArgs(args: any): args is ExecuteQueryArgs {
  return args &&
         typeof args.host === 'string' &&
         typeof args.user === 'string' &&
         typeof args.password === 'string' &&
         typeof args.database === 'string' &&
         typeof args.query === 'string' &&
         (args.port === undefined || typeof args.port === 'number') &&
         (args.params === undefined || Array.isArray(args.params));
}

const server = new Server(
  {
    name: "database-server",
    version: "0.1.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// 注册工具列表
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "get_table_columns",
        description: "获取数据库表的列信息，包括列名、类型、是否可空、注释等详细信息",
        inputSchema: {
          type: "object",
          properties: {
            host: {
              type: "string",
              description: "数据库主机地址"
            },
            user: {
              type: "string",
              description: "数据库用户名"
            },
            password: {
              type: "string",
              description: "数据库密码"
            },
            database: {
              type: "string",
              description: "数据库名称"
            },
            tableName: {
              type: "string",
              description: "表名"
            },
            port: {
              type: "number",
              description: "数据库端口号，默认3306",
              default: 3306
            }
          },
          required: ["host", "user", "password", "database", "tableName"]
        }
      },
      {
        name: "get_tables",
        description: "获取数据库中所有表的信息，包括表名、注释、类型、引擎等",
        inputSchema: {
          type: "object",
          properties: {
            host: {
              type: "string",
              description: "数据库主机地址"
            },
            user: {
              type: "string",
              description: "数据库用户名"
            },
            password: {
              type: "string",
              description: "数据库密码"
            },
            database: {
              type: "string",
              description: "数据库名称"
            },
            port: {
              type: "number",
              description: "数据库端口号，默认3306",
              default: 3306
            }
          },
          required: ["host", "user", "password", "database"]
        }
      },
      {
        name: "execute_query",
        description: "执行自定义SQL查询语句",
        inputSchema: {
          type: "object",
          properties: {
            host: {
              type: "string",
              description: "数据库主机地址"
            },
            user: {
              type: "string",
              description: "数据库用户名"
            },
            password: {
              type: "string",
              description: "数据库密码"
            },
            database: {
              type: "string",
              description: "数据库名称"
            },
            query: {
              type: "string",
              description: "SQL查询语句"
            },
            params: {
              type: "array",
              description: "查询参数（可选）",
              items: {
                type: "string"
              }
            },
            port: {
              type: "number",
              description: "数据库端口号，默认3306",
              default: 3306
            }
          },
          required: ["host", "user", "password", "database", "query"]
        }
      },
      {
        name: "test_connection",
        description: "测试数据库连接是否正常",
        inputSchema: {
          type: "object",
          properties: {
            host: {
              type: "string",
              description: "数据库主机地址"
            },
            user: {
              type: "string",
              description: "数据库用户名"
            },
            password: {
              type: "string",
              description: "数据库密码"
            },
            database: {
              type: "string",
              description: "数据库名称"
            },
            port: {
              type: "number",
              description: "数据库端口号，默认3306",
              default: 3306
            }
          },
          required: ["host", "user", "password", "database"]
        }
      }
    ]
  };
});

// 处理工具调用
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  
  // 参数验证
  if (!args || typeof args !== 'object') {
    return {
      content: [
        {
          type: "text",
          text: "Error: Missing or invalid arguments"
        }
      ],
      isError: true
    };
  }
  
  try {
    let result;
    
    switch (name) {
      case "get_table_columns": {
        if (!isGetTableColumnsArgs(args)) {
          throw new Error("Invalid arguments for get_table_columns. Required: host, user, password, database, tableName");
        }
        result = await getTableColumns(
          args.host,
          args.user,
          args.password,
          args.database,
          args.tableName,
          args.port
        );
        break;
      }
        
      case "get_tables": {
        if (!isDatabaseConnectionArgs(args)) {
          throw new Error("Invalid arguments for get_tables. Required: host, user, password, database");
        }
        result = await getTables(
          args.host,
          args.user,
          args.password,
          args.database,
          args.port
        );
        break;
      }
        
      case "execute_query": {
        if (!isExecuteQueryArgs(args)) {
          throw new Error("Invalid arguments for execute_query. Required: host, user, password, database, query");
        }
        result = await executeQuery(
          args.host,
          args.user,
          args.password,
          args.database,
          args.query,
          args.params,
          args.port
        );
        break;
      }
        
      case "test_connection": {
        if (!isDatabaseConnectionArgs(args)) {
          throw new Error("Invalid arguments for test_connection. Required: host, user, password, database");
        }
        result = await testConnection(
          args.host,
          args.user,
          args.password,
          args.database,
          args.port
        );
        break;
      }
        
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
    
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(result, null, 2)
        }
      ]
    };
    
  } catch (error) {
    return {
      content: [
        {
          type: "text",
          text: `Error: ${error instanceof Error ? error.message : 'Unknown error'}`
        }
      ],
      isError: true
    };
  }
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Database MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Server error:", error);
  process.exit(1);
});