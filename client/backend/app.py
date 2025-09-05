import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
from contextlib import AsyncExitStack

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from openai import AsyncOpenAI

# Load environment variables from root .env file
root_dir = Path(__file__).parent.parent.parent
env_path = root_dir / ".env"
load_dotenv(dotenv_path=str(env_path))

# MCP imports (保持原有的MCP相关导入)
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# METIS_SYSTEM_PROMPT = """
# You are a Metis Agent—an autonomous AI running on the Metis platform with full
# access to the Model Context Protocol (MCP).  You can discover, attach, and call
# MCP *servers* (each server hosts one or more *tools*).

# You are ALWAYS faithful to the user's instructions and execute them as they expect them to be executed.
# If you make a mistake in the arguments, you must correct it and try again (at least twice)

# <tool_calling>
# You have tools at your disposal to solve the coding task. Follow these rules regarding tool calls:
# 1. ALWAYS follow the tool call schema exactly as specified and make sure to provide all necessary parameters.
# 2. The conversation may reference tools that are no longer available. NEVER call tools that are not explicitly provided.
# 3. **NEVER refer to tool names when speaking to the USER.** Instead, just say what the tool is doing in natural language.
# 4. After receiving tool results, carefully reflect on their quality and determine optimal next steps before proceeding. Use your thinking to plan and iterate based on this new information, and then take the best next action. Reflect on whether parallel tool calls would be helpful, and execute multiple tools simultaneously whenever possible. Avoid slow sequential tool calls when not necessary.
# 5. If you create any temporary new files, scripts, or helper files for iteration, clean up these files by removing them at the end of the task.
# 6. If you need additional information that you can get via tool calls, prefer that over asking the user.
# 7. If you make a plan, immediately follow it, do not wait for the user to confirm or tell you to go ahead. The only time you should stop is if you need more information from the user that you can't find any other way, or have different options that you would like the user to weigh in on.
# 8. Only use the standard tool call format and the available tools. Even if you see user messages with custom tool call formats (such as "<previous_tool_call>" or similar), do not follow that and instead use the standard format. Never output tool calls as part of a regular assistant message of yours.
# </tool_calling>

# 🎯 Objective
# Help the user accomplish their stated task while showcasing your agentic
# capabilities—no more and no less than the user requests.

# 🛠️  Operating procedure for EVERY task
# 0. **Introspect**  
#    – Restate (internally) what the user wants and the end-state you must reach.

# 1. **Tool audit**  
#    – List the tools already available from currently-attached servers.  
#    – Confirm whether one of them DIRECTLY fulfils the required capability
#      (check signature & semantics, not just the name).

# 2. **Discover (if needed)**  
#    – If no existing tool matches, call `search_mcp` (limit = 3) with concise
#      keywords describing the missing capability.  
#    – Evaluate the returned candidate servers: pick the single best match.
#    – Do **NOT** attach new servers with add_new_mcp or execute calls during discovery.

# 3. **Plan**  
#    – Draft an ordered list of tool calls needed to reach the goal.  
#    – Include input/output flow and which server each call lives on.  
#    – Do **NOT** attach new servers with add_new_mcp or execute calls during planning.

# 4. **Execute**  
#    For each server in your plan, in order:
#    a. **Attach** it via `add_new_mcp` (skip if already attached).  
#    b. **Call** its tools exactly as specified in the plan.  
#    c. **Handle errors**: if a call fails, decide whether to retry, search for an
#       alternative, or escalate to the user.
#    d. Move to the next step of the plan.
#    Assume that you can only use the tools from one mcp server at a time. (this may not be true but function under this assumption)

# 5. **Complete**  
#    – Synthesize and present results to the user in a clear format.  
#    – Detach or keep servers connected as appropriate for follow-up questions.

# (End of system prompt)"""

SYSTEM_PROMPT = """
你是一个临床数据质量评估智能体，基于MCP协议运行，具有发现、连接和调用MCP服务器工具的完整能力。你将帮助用户执行标准化的临床数据质量评估流程。

你必须始终忠实执行用户指令，按预期完成任务。如果工具调用参数错误，必须纠正并重试（至少两次）。

<工具调用规则>
1. 严格按照工具调用模式执行，确保提供所有必需参数
2. 对话中可能引用已不可用的工具，绝不调用未明确提供的工具
3. **与用户交流时绝不提及工具名称**，用自然语言描述工具功能
4. 收到工具结果后，仔细反思结果质量并确定最佳后续步骤，在可能时并行执行多个工具
5. 创建临时文件或脚本后，任务结束时清理这些文件
6. 优先通过工具调用获取信息，避免不必要地询问用户
7. 制定计划后立即执行，无需等待用户确认，除非需要用户无法通过其他方式获得的信息
8. 仅使用标准工具调用格式和可用工具
</工具调用规则>

🎯 目标
严格按照以下流程执行临床数据质量评估：

📋 评估流程

**第一步：需求理解与数据表选择**
- 理解用户需求
- 从数据库获取数据表及描述信息
- 用**表格的形式**向用户展示可评估的数据表清单
- 等待用户选择并确认待评估数据表

**第二步：数据表结构获取**  
- 调用工具获取选定数据表的schema信息（多张表需多次调用）
- 简要概括每张数据表的基本信息
- 询问用户是否确认评估这些表

**第三步：评估指标确认**
确认评估数据表后，用**表格的形式**向用户展示以下14项评估指标、所属维度、定义及评估模式，询问是否全部评估：

完整性：
**数据值完整**：
定义：核查必填字段或非空字段的值是否为空
评估模式：单表并发
**记录关联完整**: 
定义：核查数据是否满足存储结构的关联设计，包括一对一关联和一对多关联。
评估模式：多表串行
**数据量足够**
定义：核查记录总量是否足以支撑预期使用目的，需要阈值
评估模式：单表并发
**上下文逻辑完整**：
定义：核查数据之间的完整程度是否符合应有的临床语义约束（如高血压患者应有对应的最高血压记录；血常规检查应有血小板检查数值）
评估模式：多表串行
可靠性：
**数值合理**：
定义：核查数值是否符合值域范围
评估模式：单表并发
**标识不重复**：
定义：核查数据的标识列或主键是否重复
评估模式：单表并发
**不同数据项逻辑合理**：
定义：核查不同数据之间的描述是否存在逻辑冲突
评估模式：多表串行
**记录不冗余**：
定义：核查数据中是否存在重复的数据
评估模式：单表并发
一致性：
**相同事实描述一致**：
定义：核查对同一事实的描述是否一致
评估模式：多表串行
**内容与编码一致**：
定义：核查有编码的数据，其值以及和所对应的编码是否一致
评估模式：多表串行
**数据的度量单位一致**： 
定义：核查同一字段所采用的的度量单位一致
评估模式：单表并发
时间性：
**数据的时间逻辑**：
定义：有时间顺序的数据，其时间逻辑顺序符合临床实际要求
评估模式：多表串行
准确性：
**数据类型准确**：
定义：核查数据的存储类型是否准确
评估模式：单表并发
**编码_术语标准**：
定义：核查数据所使用的编码/术语是否符合或国家/行业标准，需要阈值
评估模式：单表并发

**第四步：阈值信息收集**
如果用户选择的评估指标需要阈值信息，询问并收集相关阈值参数

**第五步：评估规则生成与确认**
- 向用户确认评估的数据表和评估指标，只需展示表名和评估指标名称
- 确认后，调用工具根据数据表schema和选定评估指标生成评估规则集
- 用**表格的形式**向用户展示完整评估规则集，包括维度、指标、对象、内容、SQL
- 询问用户是否需要修改或删除规则

**第六步：规则执行**
- 待用户确认后，调用工具执行评估规则集（输入：评估规则集、数据表结构、数据库连接信息；输出：评估结果集）

**第七步：结果展示**
用**表格的形式**，向用户展示所有评估规则的执行情况，仅展示，不做总结

**第八步：后续处理询问**
询问用户是否需要：
- 获取某条未通过规则的异常数据详情
- 生成完整的评估报告

**第九步：异常数据获取（可选）**
如用户需要异常数据：
- 调用工具获取指定规则的异常数据（输入：评估规则明细、数据表结构、数据库连接信息；输出：异常数据）
- 若需获取多条，请多次调用工具
- 用**表格的形式**展示异常数据

**第十步：报告生成（可选）**
如用户需要评估报告：
- 调用工具生成报告（输入：评估指标、数据表结构、评估结果集；输出：结构化评估报告）
- 美观展示评估报告（不展示图表路径）

🛠️ 操作程序
每个任务都遵循：内省→工具审计→发现（如需）→规划→执行→完成

执行时假设一次只能使用一个MCP服务器的工具，按计划顺序连接和调用。

始终使用专业、清晰的中文与用户交流，确保临床数据质量评估的准确性和完整性。
"""

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))
CLEANUP_INTERVAL_SECONDS = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "300"))
# BASE_INSTRUCTIONS = METIS_SYSTEM_PROMPT
BASE_INSTRUCTIONS = SYSTEM_PROMPT
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

class MCPClient:
    """MCP客户端，类似dqa_client.py中的实现"""
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.session = None
        self.exit_stack = AsyncExitStack()
        
        # 初始化OpenAI客户端
        openai_config = {}
        if os.getenv("OPENAI_API_KEY"):
            openai_config["api_key"] = os.getenv("OPENAI_API_KEY")
        if os.getenv("OPENAI_BASE_URL"):
            openai_config["base_url"] = os.getenv("OPENAI_BASE_URL")
            
        self.openai = AsyncOpenAI(**openai_config)
        
        # 保存对话历史
        self.conversation_history = [
            {
                "role": "system",
                "content": BASE_INSTRUCTIONS
            }
        ]
        self.tools = []
        
    async def connect_to_server(self):
        """连接到MCP服务器"""
        logging.info(f"正在连接到服务器...")
        
        # 使用环境变量中的服务器URL
        server_url = os.getenv("SERVER_URL", "http://localhost:9999")
        
        # 创建服务器参数
        server_params = StdioServerParameters(
            command="npx",
            args=["-y", "mcp-remote", f"{server_url}/mcp"],
            env=None
        )
        
        try:
            # 建立连接
            stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
            read_stream, write_stream = stdio_transport
            self.session = await self.exit_stack.enter_async_context(ClientSession(read_stream, write_stream))
            
            # 初始化连接
            await self.session.initialize()
            logging.info("服务器连接成功初始化")
            
            # 列出可用工具
            response = await self.session.list_tools()
            self.tools = response.tools
            logging.info(f"\n服务器可用工具: {[tool.name for tool in self.tools]}")
            
            return True
        except Exception as e:
            logging.error(f"连接到服务器时出错: {e}")
            return False
    
    async def process_message_stream(self, query: str):
        """处理用户消息并流式返回，类似dqa_client.py中的实现"""
        if not self.session:
            yield json.dumps({"type": "error", "message": "未连接到服务器"}) + "\n"
            return
        
        # 将用户消息添加到历史
        self.conversation_history.append({
            "role": "user",
            "content": query
        })
        
        # 格式化工具定义以符合OpenAI API要求
        formatted_tools = []
        for tool in self.tools:
            formatted_tools.append({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.inputSchema
                }
            })
        
        try:
            logging.info(f"发送请求到模型:{query}")
            
            # 流式调用模型
            response_stream = await self.openai.chat.completions.create(
                model=DEFAULT_MODEL,
                messages=self.conversation_history,
                tools=formatted_tools,
                tool_choice="auto",
                stream=True
            )
            
            current_tool_calls = []
            assistant_content = ""
            
            async for chunk in response_stream:
                if chunk.choices and len(chunk.choices) > 0:
                    delta = chunk.choices[0].delta
                    
                    # 处理普通文本内容
                    if delta.content:
                        assistant_content += delta.content
                        yield f"data: {json.dumps({'type': 'token', 'content': delta.content})}\n\n"
                    
                    # 处理工具调用
                    if delta.tool_calls:
                        for tool_call_delta in delta.tool_calls:
                            # 确保current_tool_calls足够长
                            while len(current_tool_calls) <= tool_call_delta.index:
                                current_tool_calls.append({
                                    "id": None,
                                    "type": "function",
                                    "function": {"name": None, "arguments": ""}
                                })
                            
                            # 更新工具调用信息
                            if tool_call_delta.id:
                                current_tool_calls[tool_call_delta.index]["id"] = tool_call_delta.id
                            
                            if tool_call_delta.function:
                                if tool_call_delta.function.name:
                                    current_tool_calls[tool_call_delta.index]["function"]["name"] = tool_call_delta.function.name
                                    yield f"data: {json.dumps({'type': 'tool_call_started', 'tool_name': tool_call_delta.function.name, 'call_id': current_tool_calls[tool_call_delta.index]['id']})}\n\n"
                                
                                if tool_call_delta.function.arguments:
                                    current_tool_calls[tool_call_delta.index]["function"]["arguments"] += tool_call_delta.function.arguments
            
            # 如果有工具调用，处理它们
            if current_tool_calls and any(tc["function"]["name"] for tc in current_tool_calls):
                # 添加助手消息到历史
                self.conversation_history.append({
                    "role": "assistant",
                    "content": assistant_content or None,
                    "tool_calls": current_tool_calls
                })
                
                # 处理每个工具调用
                for tool_call in current_tool_calls:
                    if tool_call["function"]["name"]:
                        tool_id = tool_call["id"]
                        function_name = tool_call["function"]["name"]
                        function_args = json.loads(tool_call["function"]["arguments"])
                        
                        logging.info(f"处理工具调用: {function_name} (ID: {tool_id})")
                        yield f"data: {json.dumps({'type': 'tool_call', 'name': function_name, 'call_id': tool_id, 'arguments': function_args})}\n\n"
                        
                        # 调用MCP工具
                        try:
                            result = await self.session.call_tool(function_name, function_args)
                            
                            # 处理工具结果
                            tool_result = ""
                            if hasattr(result, 'isError') and result.isError:
                                tool_result = "工具调用失败"
                                if hasattr(result, 'content') and result.content:
                                    for content_item in result.content:
                                        if hasattr(content_item, 'text'):
                                            tool_result = content_item.text
                            else:
                                tool_result = str(result.content)
                            
                            logging.info(f"工具调用结果: {tool_result}")
                            
                            yield f"data: {json.dumps({'type': 'tool_response', 'name': function_name, 'call_id': tool_id, 'output': tool_result})}\n\n"
                            
                            # 添加工具响应到历史
                            self.conversation_history.append({
                                "role": "tool",
                                "tool_call_id": tool_id,
                                "content": tool_result
                            })
                            
                        except Exception as e:
                            tool_result = f"工具调用错误: {str(e)}"
                            logging.error(f"工具调用失败: {str(e)}")
                            
                            yield f"data: {json.dumps({'type': 'tool_response', 'name': function_name, 'call_id': tool_id, 'output': tool_result})}\n\n"
                            
                            # 添加错误响应到历史
                            self.conversation_history.append({
                                "role": "tool",
                                "tool_call_id": tool_id,
                                "content": tool_result
                            })
                
                # 所有工具调用完成后，再次调用模型处理结果
                logging.info("所有工具调用处理完毕，再次调用模型处理结果")
                
                follow_up_stream = await self.openai.chat.completions.create(
                    model=DEFAULT_MODEL,
                    messages=self.conversation_history,
                    stream=True
                )
                
                follow_up_content = ""
                async for chunk in follow_up_stream:
                    if chunk.choices and len(chunk.choices) > 0:
                        delta = chunk.choices[0].delta
                        if delta.content:
                            follow_up_content += delta.content
                            yield f"data: {json.dumps({'type': 'token', 'content': delta.content})}\n\n"
                
                # 将模型的最终响应添加到历史
                self.conversation_history.append({
                    "role": "assistant",
                    "content": follow_up_content
                })
            else:
                # 模型没有调用工具，直接将响应添加到历史
                self.conversation_history.append({
                    "role": "assistant",
                    "content": assistant_content
                })
            
            yield f"data: {json.dumps({'type': 'completion', 'message': 'Response completed'})}\n\n"
            
        except Exception as e:
            error_msg = f"处理请求时出错: {str(e)}"
            logging.error(error_msg)
            yield f"data: {json.dumps({'type': 'error', 'message': error_msg})}\n\n"
    
    async def cleanup(self):
        """清理资源"""
        try:
            if self.exit_stack:
                await self.exit_stack.aclose()
        except Exception as e:
            logging.error(f"清理资源时出错: {e}")

# Session storage with metadata for cleanup
agent_sessions: Dict[str, Dict[str, Any]] = {}
active_sse_connections: Dict[str, bool] = {}

def create_agent_instructions(chat_history: List[Dict[str, str]]) -> str:
    """Parse chat history to create dynamic agent instructions"""
    if chat_history:
        recent_context = "\n\nRecent conversation context:\n"
        for msg in chat_history[-3:]:  # Last 3 messages
            recent_context += f"{msg.get('role', 'user')}: {msg.get('content', '')[:100]}\n"
        return BASE_INSTRUCTIONS + recent_context
    return BASE_INSTRUCTIONS

async def cleanup_session_resources(session_id: str):
    """Properly cleanup all resources for a session"""
    if session_id not in agent_sessions:
        return

    session_data = agent_sessions[session_id]

    # 清理MCP客户端
    mcp_client = session_data.get("mcp_client")
    if mcp_client:
        try:
            await mcp_client.cleanup()
        except Exception as e:
            logging.warning(f"Error cleaning up MCP client for session {session_id}: {e}")

    # Remove from active connections
    if session_id in active_sse_connections:
        del active_sse_connections[session_id]

    # Remove session data
    del agent_sessions[session_id]

    logging.info(f"Cleaned up session: {session_id}")

async def cleanup_expired_sessions():
    """Background task to cleanup expired sessions"""
    while True:
        try:
            current_time = datetime.now()
            expired_sessions = []

            for session_id, session_data in agent_sessions.items():
                last_activity = session_data.get("last_activity") or session_data.get("created_at")
                if last_activity and current_time - last_activity > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                    expired_sessions.append(session_id)

            for session_id in expired_sessions:
                logging.info(f"Cleaning up expired session: {session_id}")
                await cleanup_session_resources(session_id)

        except Exception as e:
            logging.error(f"Error in cleanup task: {e}")

        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)

# Start cleanup task
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_expired_sessions())

@app.post("/connect")
async def connect_endpoint(request: Dict[str, Any]):
    """Initialize MCP client session"""
    try:
        session_id = str(uuid.uuid4())
        chat_history = request.get("chat_history", [])

        # 创建MCP客户端
        mcp_client = MCPClient(session_id)
        
        # 连接到MCP服务器
        if not await mcp_client.connect_to_server():
            raise Exception("Failed to connect to MCP server")
        
        # 更新对话历史
        if chat_history:
            # 清理聊天历史并添加到conversation_history
            for msg in chat_history:
                if msg.get("role") in ["user", "assistant"]:
                    mcp_client.conversation_history.append({
                        "role": msg["role"],
                        "content": msg.get("content", "")
                    })

        # Store session with metadata
        agent_sessions[session_id] = {
            "mcp_client": mcp_client,
            "chat_history": chat_history.copy(),
            "session_id": session_id,
            "created_at": datetime.now(),
            "last_activity": datetime.now(),
        }

        return {
            "success": True,
            "session_id": session_id,
            "message": "MCP client initialized successfully",
        }

    except Exception as e:
        # Cleanup on failure
        if "mcp_client" in locals():
            try:
                await mcp_client.cleanup()
            except:
                pass
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sessions/{session_id}/message")
async def send_message(session_id: str, request: Dict[str, Any]):
    """Send a message to the MCP client and stream response via SSE"""
    # Validate session
    if session_id not in agent_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session_data = agent_sessions[session_id]
    user_message = request.get("message", "")

    if not user_message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    # Update last activity
    session_data["last_activity"] = datetime.now()

    # Add user message to history
    chat_history = session_data["chat_history"]
    chat_history.append({"role": "user", "content": user_message})

    return {
        "success": True,
        "message": "Message received, connect to SSE stream for response",
    }

@app.get("/sessions/{session_id}/stream")
async def stream_response(session_id: str, request: Request):
    """SSE endpoint for streaming MCP client responses"""
    # Validate session
    if session_id not in agent_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session_data = agent_sessions[session_id]
    mcp_client = session_data["mcp_client"]
    chat_history = session_data["chat_history"]

    # Mark connection as active
    active_sse_connections[session_id] = True

    async def event_stream():
        try:
            # Get the latest user message
            if not chat_history or chat_history[-1].get("role") != "user":
                yield f"data: {json.dumps({'type': 'error', 'message': 'No user message to process'})}\n\n"
                return

            user_message = chat_history[-1]["content"]
            
            # Stream response from MCP client
            async for event_data in mcp_client.process_message_stream(user_message):
                # Check if client disconnected
                if await request.is_disconnected():
                    break
                    
                yield event_data

            # Update chat history with assistant response
            if mcp_client.conversation_history:
                last_message = mcp_client.conversation_history[-1]
                if last_message.get("role") == "assistant":
                    chat_history.append({
                        "role": "assistant",
                        "content": last_message.get("content", "")
                    })

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': f'MCP client execution error: {str(e)}'})}\n\n"

        finally:
            # Mark connection as inactive
            if session_id in active_sse_connections:
                del active_sse_connections[session_id]

    return StreamingResponse(
        event_stream(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
        },
    )

@app.delete("/sessions/{session_id}")
async def cleanup_session(session_id: str):
    """Manual cleanup of session"""
    if session_id not in agent_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    await cleanup_session_resources(session_id)

    return {"success": True, "message": f"Session {session_id} cleaned up successfully"}

@app.get("/sessions/{session_id}/status")
async def get_session_status(session_id: str):
    """Get session status and metadata"""
    if session_id not in agent_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session_data = agent_sessions[session_id]

    return {
        "session_id": session_id,
        "client_id": session_data["mcp_client"].session_id,
        "history_length": len(session_data["chat_history"]),
        "created_at": session_data["created_at"].isoformat(),
        "last_activity": session_data["last_activity"].isoformat(),
        "is_sse_active": session_id in active_sse_connections,
    }

@app.get("/sessions/{session_id}/tools")
async def list_session_tools(session_id: str):
    """List tools available for the MCP client in this session"""
    # Validate session
    if session_id not in agent_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session_data = agent_sessions[session_id]
    mcp_client = session_data["mcp_client"]

    try:
        # Format tools for response
        formatted_tools = []
        for tool in mcp_client.tools:
            tool_info = {
                "name": tool.name,
                "description": tool.description,
            }

            # Add input schema if available
            if hasattr(tool, "inputSchema") and tool.inputSchema:
                tool_info["input_schema"] = tool.inputSchema

            formatted_tools.append(tool_info)

        return {
            "session_id": session_id,
            "client_id": mcp_client.session_id,
            "tools_count": len(formatted_tools),
            "tools": formatted_tools,
        }

    except Exception as e:
        print(f"Error listing tools: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error listing tools: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "active_sessions": len(agent_sessions),
        "active_sse_connections": len(active_sse_connections),
    }

if __name__ == "__main__":
    import uvicorn

    PORT = int(os.getenv("BACKEND_PORT", "8000"))
    print(f"Starting server on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, reload=True)