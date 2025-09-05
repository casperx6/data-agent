#!/usr/bin/env ts-node
import mysql from 'mysql2/promise';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// Load environment variables from root .env file
dotenv.config({ path: path.resolve('../.env') });

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Database connection configuration interface
interface DatabaseConfig {
  host: string;
  user: string;
  password: string;
  database: string;
  port?: number;
}

// Column information interface
interface ColumnInfo {
  columnName: string;
  columnType: string;
  isNullable: string;
  columnComment: string;
  columnDefault: string | null;
  extra: string;
}

// Table information interface
interface TableInfo {
  tableName: string;
  tableComment: string;
  tableType: string;
  engine: string;
  tableRows: number | null;
}

// Database operation result interface
interface DatabaseResult<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message: string;
}

// Create database connection
async function createConnection(config: DatabaseConfig): Promise<mysql.Connection> {
  try {
    const connection = await mysql.createConnection({
      host: config.host,
      user: config.user,
      password: config.password,
      database: config.database,
      port: config.port || 3306,
    });
    
    return connection;
  } catch (error) {
    throw new Error(`Failed to connect to database: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

// Get table columns information (equivalent to the Python function)
export async function getTableColumns(
  host: string,
  user: string,
  password: string,
  database: string,
  tableName: string,
  port?: number
): Promise<DatabaseResult<ColumnInfo[]>> {
  console.log(`🔍 Getting columns for table: ${tableName} in database: ${database}`);
  
  let connection: mysql.Connection | null = null;
  
  try {
    // Create database connection
    connection = await createConnection({
      host,
      user,
      password,
      database,
      port
    });
    
    // Execute SQL query to get column information
    const sql = `
      SELECT 
        column_name as columnName,
        column_type as columnType,
        is_nullable as isNullable,
        IFNULL(column_comment, '') as columnComment,
        column_default as columnDefault,
        extra
      FROM information_schema.columns 
      WHERE table_schema = ? AND table_name = ?
      ORDER BY ordinal_position
    `;
    
    const [rows] = await connection.execute(sql, [database, tableName]);
    const columns = rows as ColumnInfo[];
    
    if (columns.length > 0) {
      console.log(`   - Found ${columns.length} columns in table ${tableName}`);
      return {
        success: true,
        data: columns,
        message: `Successfully retrieved ${columns.length} columns from table ${tableName}`
      };
    } else {
      return {
        success: false,
        message: `Table ${tableName} does not exist or has no columns in database ${database}`
      };
    }
    
  } catch (error) {
    console.error('Error getting table columns:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error occurred',
      message: `Failed to get columns for table ${tableName}: ${error instanceof Error ? error.message : 'Unknown error'}`
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Get all tables in database
export async function getTables(
  host: string,
  user: string,
  password: string,
  database: string,
  port?: number
): Promise<DatabaseResult<TableInfo[]>> {
  console.log(`📊 Getting tables from database: ${database}`);
  
  let connection: mysql.Connection | null = null;
  
  try {
    connection = await createConnection({
      host,
      user,
      password,
      database,
      port
    });
    
    const sql = `
      SELECT 
        table_name as tableName,
        IFNULL(table_comment, '') as tableComment,
        table_type as tableType,
        IFNULL(engine, '') as engine,
        table_rows as tableRows
      FROM information_schema.tables 
      WHERE table_schema = ?
      ORDER BY table_name
    `;
    
    const [rows] = await connection.execute(sql, [database]);
    const tables = rows as TableInfo[];
    
    console.log(`   - Found ${tables.length} tables in database ${database}`);
    return {
      success: true,
      data: tables,
      message: `Successfully retrieved ${tables.length} tables from database ${database}`
    };
    
  } catch (error) {
    console.error('Error getting tables:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error occurred',
      message: `Failed to get tables from database ${database}: ${error instanceof Error ? error.message : 'Unknown error'}`
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Execute custom SQL query
export async function executeQuery(
  host: string,
  user: string,
  password: string,
  database: string,
  query: string,
  params?: any[],
  port?: number
): Promise<DatabaseResult> {
  console.log(`⚡ Executing query: ${query.substring(0, 100)}...`);
  
  let connection: mysql.Connection | null = null;
  
  try {
    connection = await createConnection({
      host,
      user,
      password,
      database,
      port
    });
    
    const [rows, fields] = await connection.execute(query, params);
    
    return {
      success: true,
      data: { rows, fields },
      message: `Query executed successfully`
    };
    
  } catch (error) {
    console.error('Error executing query:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error occurred',
      message: `Failed to execute query: ${error instanceof Error ? error.message : 'Unknown error'}`
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Test database connection
export async function testConnection(
  host: string,
  user: string,
  password: string,
  database: string,
  port?: number
): Promise<DatabaseResult> {
  console.log(`🔌 Testing connection to database: ${database}@${host}`);
  
  let connection: mysql.Connection | null = null;
  
  try {
    connection = await createConnection({
      host,
      user,
      password,
      database,
      port
    });
    
    // Test with a simple query
    await connection.execute('SELECT 1');
    
    return {
      success: true,
      message: `Successfully connected to database ${database}@${host}`
    };
    
  } catch (error) {
    console.error('Connection test failed:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error occurred',
      message: `Failed to connect to database ${database}@${host}: ${error instanceof Error ? error.message : 'Unknown error'}`
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Format column information for display
function formatColumnInfo(columns: ColumnInfo[]): string {
  if (columns.length === 0) {
    return 'No columns found.';
  }
  
  let result = `Table contains ${columns.length} columns:\n`;
  result += '═'.repeat(80) + '\n';
  
  columns.forEach((column, index) => {
    result += `${index + 1}. ${column.columnName}\n`;
    result += `   Type: ${column.columnType}\n`;
    result += `   Nullable: ${column.isNullable === 'YES' ? '✅ Yes' : '❌ No'}\n`;
    result += `   Default: ${column.columnDefault || 'NULL'}\n`;
    result += `   Comment: ${column.columnComment || 'No comment'}\n`;
    if (column.extra) {
      result += `   Extra: ${column.extra}\n`;
    }
    result += '─'.repeat(40) + '\n';
  });
  
  return result;
}

// Format table information for display
function formatTableInfo(tables: TableInfo[]): string {
  if (tables.length === 0) {
    return 'No tables found.';
  }
  
  let result = `Database contains ${tables.length} tables:\n`;
  result += '═'.repeat(80) + '\n';
  
  tables.forEach((table, index) => {
    result += `${index + 1}. ${table.tableName}\n`;
    result += `   Type: ${table.tableType}\n`;
    result += `   Engine: ${table.engine}\n`;
    result += `   Rows: ${table.tableRows !== null ? table.tableRows.toLocaleString() : 'Unknown'}\n`;
    result += `   Comment: ${table.tableComment || 'No comment'}\n`;
    result += '─'.repeat(40) + '\n';
  });
  
  return result;
}

// Command line interface for testing
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  
  if (args.length < 5) {
    console.log('Usage: npx tsx database-mcp.ts <command> <host> <user> <password> <database> [extra_params...]');
    console.log('');
    console.log('Commands:');
    console.log('  test           - Test database connection');
    console.log('  tables         - List all tables');
    console.log('  columns        - Get table columns (requires table_name as extra param)');
    console.log('  query          - Execute SQL query (requires sql as extra param)');
    console.log('');
    console.log('Examples:');
    console.log('  npx tsx database-mcp.ts test localhost root password mydb');
    console.log('  npx tsx database-mcp.ts tables localhost root password mydb');
    console.log('  npx tsx database-mcp.ts columns localhost root password mydb users');
    console.log('  npx tsx database-mcp.ts query localhost root password mydb "SELECT COUNT(*) FROM users"');
    process.exit(1);
  }
  
  const [command, host, user, password, database, ...extraParams] = args;
  
  console.log(`🚀 Starting database operation...`);
  console.log(`📝 Command: ${command}`);
  console.log(`🏠 Host: ${host}`);
  console.log(`👤 User: ${user}`);
  console.log(`🗄️  Database: ${database}`);
  console.log('');
  
  try {
    let result: DatabaseResult;
    
    switch (command) {
      case 'test':
        result = await testConnection(host, user, password, database);
        break;
        
      case 'tables':
        result = await getTables(host, user, password, database);
        if (result.success && result.data) {
          console.log(formatTableInfo(result.data));
        }
        break;
        
      case 'columns':
        if (extraParams.length === 0) {
          console.error('❌ Table name is required for columns command');
          process.exit(1);
        }
        const tableName = extraParams[0];
        result = await getTableColumns(host, user, password, database, tableName);
        if (result.success && result.data) {
          console.log(formatColumnInfo(result.data));
        }
        break;
        
      case 'query':
        if (extraParams.length === 0) {
          console.error('❌ SQL query is required for query command');
          process.exit(1);
        }
        const sql = extraParams[0];
        result = await executeQuery(host, user, password, database, sql);
        if (result.success && result.data) {
          console.log('Query result:', JSON.stringify(result.data, null, 2));
        }
        break;
        
      default:
        console.error(`❌ Unknown command: ${command}`);
        process.exit(1);
    }
    
    if (result.success) {
      console.log(`✅ ${result.message}`);
    } else {
      console.error(`❌ ${result.message}`);
      if (result.error) {
        console.error(`   Error details: ${result.error}`);
      }
      process.exit(1);
    }
    
  } catch (error) {
    console.error('\n❌ Operation failed:', error);
    process.exit(1);
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error);
}