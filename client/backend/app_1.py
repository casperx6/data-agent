import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timedelta

# Load environment variables from root .env file
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# Get the path to the root .env file relative to this file
root_dir = Path(__file__).parent.parent.parent
env_path = root_dir / ".env"
load_dotenv(dotenv_path=str(env_path))

# OpenAI Agents SDK imports
from collections import defaultdict
from typing import cast

from agents import Agent, ModelSettings, Runner, set_default_openai_client
from agents.items import ToolCallOutputItem
from agents.mcp import MCPServerStdio
from openai.types.responses import (
    ResponseFunctionCallArgumentsDeltaEvent,
    ResponseFunctionCallArgumentsDoneEvent,
    ResponseFunctionToolCall,
    ResponseOutputItemAddedEvent,
    ResponseOutputItemDoneEvent,
    ResponseTextDeltaEvent,
)
from openai import AsyncOpenAI

# uvicorn app:app --host localhost --port 8000 --reload

# 
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
- 向用户展示可评估的数据表清单
- 等待用户选择并确认待评估数据表

**第二步：数据表结构获取**  
- 调用工具获取选定数据表的schema信息（多张表需多次调用）
- 以美观格式向用户展示数据表结构

**第三步：评估指标确认**
向用户展示以下14项评估指标及定义，询问是否全部评估：

1. **数据值完整**：核查必填字段或非空字段的值是否为空
2. **上下文逻辑完整**：核查数据之间的完整程度是否符合应有的临床语义约束（如高血压患者应有对应的最高血压记录；血常规检查应有血小板检查数值）
3. **数值合理**：核查数值是否符合值域范围
4. **标识不重复**：核查数据的标识列或主键是否重复
5. **不同时间测量合理**：核查对同一对象重复测量的值，其数值变化是否符合客观规律
6. **不同数据项逻辑合理**：核查不同数据之间的描述是否存在逻辑冲突
7. **记录不冗余**：核查数据中是否存在重复的数据
8. **数据表达合理**：核查数据描述中是否存在非法字符
9. **数据的时间逻辑**：有时间顺序的数据，其时间逻辑顺序符合临床实际要求
10. **数据的记录频率**：核查连续时序数据的记录频率是否符合临床常识或使用场景需求
11. **数据类型准确**：核查数据的存储类型是否准确
12. **数据的宽度（长度）准确**：数据的长度满足使用预期
13. **数据格式准确**：核查数据的格式是否满足使用预期
14. **数值符合字段定义**：核查数据字段的内容是否符合该字段的定义

**第四步：阈值信息收集**
如果用户选择的评估指标需要阈值信息，询问并收集相关阈值参数

**第五步：评估规则生成与确认**
- 调用工具根据数据表schema和选定评估指标生成评估规则集
- 以美观格式向用户展示完整评估规则集
- 询问用户是否需要修改或删除规则

**第六步：规则执行**
- 调用工具执行评估规则集（输入：评估规则集、数据表结构、数据库连接信息；输出：评估结果集）

**第七步：结果展示**
以美观格式向用户展示所有评估规则的执行情况

**第八步：后续处理询问**
询问用户是否需要：
- 获取某条未通过规则的异常数据详情
- 生成完整的评估报告

**第九步：异常数据获取（可选）**
如用户需要异常数据：
- 调用工具获取指定规则的异常数据（输入：评估规则明细、数据表结构、数据库连接信息；输出：异常数据）
- 美观展示异常数据

**第十步：报告生成（可选）**
如用户需要评估报告：
- 调用工具生成报告（输入：评估指标、数据表结构、评估结果集；输出：结构化评估报告）
- 美观展示评估报告

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

# Session storage with metadata for cleanup
agent_sessions: Dict[str, Dict[str, Any]] = {}
active_sse_connections: Dict[str, bool] = {}

# Configuration
SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))
CLEANUP_INTERVAL_SECONDS = int(
    os.getenv("CLEANUP_INTERVAL_SECONDS", "300")
)  # 5 minutes

# BASE_INSTRUCTIONS = METIS_SYSTEM_PROMPT
BASE_INSTRUCTIONS = SYSTEM_PROMPT
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

def create_agent_instructions(chat_history: List[Dict[str, str]]) -> str:
    """Parse chat history to create dynamic agent instructions"""

    if chat_history:
        recent_context = "\n\nRecent conversation context:\n"
        for msg in chat_history[-3:]:  # Last 3 messages
            recent_context += (
                f"{msg.get('role', 'user')}: {msg.get('content', '')[:100]}\n"
            )
        return BASE_INSTRUCTIONS + recent_context

    return BASE_INSTRUCTIONS


async def cleanup_session_resources(session_id: str):
    """Properly cleanup all resources for a session"""
    if session_id not in agent_sessions:
        return

    session_data = agent_sessions[session_id]

    # Close MCP server connections
    for mcp_server in session_data.get("mcp_servers", []):
        try:
            await mcp_server.cleanup()
        except Exception as e:
            logging.warning(f"Error closing MCP server for session {session_id}: {e}")

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
            expired_sessions = []  # there is no need to save expired sessions

            for session_id, session_data in agent_sessions.items():
                last_activity = session_data.get("last_activity") or session_data.get(
                    "created_at"
                )
                if last_activity and current_time - last_activity > timedelta(
                    minutes=SESSION_TIMEOUT_MINUTES
                ):
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
    """Initialize agent session"""
    try:
        session_id = str(uuid.uuid4())
        chat_history = request.get("chat_history", [])

        custom_client = AsyncOpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        )
        set_default_openai_client(custom_client)

        # Create Metis MCP server connection for this session
        server_url = os.getenv("SERVER_URL", "http://localhost:9999")
        metis_mcp_server = MCPServerStdio(
            name=f"metis-{session_id}",
            params={
                "command": "npx",
                "args": ["-y", "mcp-remote", f"{server_url}/mcp"],
            },
            client_session_timeout_seconds=300,
        )

        # Initialize MCP server connection
        await metis_mcp_server.connect()

        # Create agent with dynamic instructions and Metis MCP server access
        agent = Agent(
            name=f"metis-agent-{session_id}",
            instructions=create_agent_instructions(chat_history),
            mcp_servers=[metis_mcp_server],
            model=DEFAULT_MODEL,
            model_settings=ModelSettings(parallel_tool_calls=False),
        )

        # Store session with metadata
        agent_sessions[session_id] = {
            "agent": agent,
            "mcp_servers": [metis_mcp_server],
            "chat_history": chat_history.copy(),
            "session_id": session_id,
            "created_at": datetime.now(),
            "last_activity": datetime.now(),
        }

        return {
            "success": True,
            "session_id": session_id,
            "message": "Agent initialized successfully with Metis MCP server",
        }

    except Exception as e:
        # Cleanup on failure
        if "metis_mcp_server" in locals():
            try:
                await metis_mcp_server.cleanup()
            except:
                pass
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/message")
async def send_message(session_id: str, request: Dict[str, Any]):
    """Send a message to the agent and stream response via SSE"""
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
    """SSE endpoint for streaming agent responses"""
    # Validate session
    if session_id not in agent_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session_data = agent_sessions[session_id]
    agent = session_data["agent"]
    chat_history = session_data["chat_history"]

    # Mark connection as active
    active_sse_connections[session_id] = True

    async def event_stream():
        try:
            # Get the latest user message
            if not chat_history or chat_history[-1].get("role") != "user":
                yield f"data: {json.dumps({'type': 'error', 'message': 'No user message to process'})}\n\n"
                return

            # Prepare input for agent - clean chat history to remove non-standard fields
            if len(chat_history) > 1:
                # Clean chat history for OpenAI API - only include role and content
                cleaned_history = []
                for msg in chat_history[-10:]:  # Last 10 messages for context
                    cleaned_msg = {
                        "role": msg.get("role", "user"),
                        "content": msg.get("content", ""),
                    }
                    cleaned_history.append(cleaned_msg)
                agent_input = cleaned_history
            else:
                agent_input = chat_history[-1]["content"]

            # Track tool call buffers for this stream
            tool_call_buffers = defaultdict(str)
            tool_call_names = {}  # Map call_id to tool name
            most_recent_call_id = None  # Track the most recent call_id
            full_response = ""

            # Run agent with streaming
            print(f"\n\nRecent context: {agent_input}\n\n")
            result = Runner.run_streamed(agent, input=agent_input, max_turns=20)

            async for event in result.stream_events():
                # Check if client disconnected
                if await request.is_disconnected():
                    break

                if event.type == "raw_response_event":
                    data = event.data

                    if isinstance(data, ResponseTextDeltaEvent):
                        token = data.delta
                        full_response += token
                        yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"

                    elif isinstance(data, ResponseFunctionCallArgumentsDeltaEvent):
                        tool_call_buffers[data.item_id] += data.delta

                    elif isinstance(data, ResponseFunctionCallArgumentsDoneEvent):
                        args = tool_call_buffers.pop(data.item_id, data.arguments)
                        tool_name = tool_call_names.get(most_recent_call_id)

                        try:
                            parsed_args = json.loads(args)
                            full_response += (
                                f"\n[Tool Call] Arguments: {json.dumps(parsed_args)}\n"
                            )
                            yield f"data: {json.dumps({'type': 'tool_call', 'name': tool_name, 'call_id': most_recent_call_id, 'arguments': parsed_args})}\n\n"
                        except Exception:
                            full_response += f"\n[Tool Call] Arguments: {args}\n"
                            yield f"data: {json.dumps({'type': 'tool_call', 'name': tool_name, 'call_id': most_recent_call_id, 'arguments': args})}\n\n"

                    elif isinstance(data, ResponseOutputItemDoneEvent):
                        if (
                            hasattr(data.item, "name")
                            and hasattr(data.item, "type")
                            and getattr(data.item, "type") == "function_call"
                        ):
                            # Handle completed tool call with name
                            tool_call = cast(ResponseFunctionToolCall, data.item)
                            full_response += f"[Tool Call] Name: {tool_call.name}\n"
                            call_id = tool_call.call_id
                            yield f"data: {json.dumps({'type': 'tool_call_complete', 'name': tool_call.name, 'call_id': call_id})}\n\n"
                        elif isinstance(data.item, ToolCallOutputItem):
                            # Handle tool response - need to find the item_id that corresponds to this output
                            output_str = data.item.output if data.item.output else ""

                            # Get the call_id from raw_item if available
                            call_id = (
                                data.item.raw_item.get("call_id")
                                if hasattr(data.item, "raw_item")
                                else None
                            )
                            tool_name = tool_call_names.get(call_id)

                            try:
                                output = json.loads(output_str)
                                full_response += f"[Tool Response] {output.get('text', output_str)}\n"
                                yield f"data: {json.dumps({'type': 'tool_response', 'name': tool_name, 'call_id': call_id, 'output': output})}\n\n"
                            except Exception:
                                full_response += f"[Tool Response] {output_str}\n"
                                yield f"data: {json.dumps({'type': 'tool_response', 'name': tool_name, 'call_id': call_id, 'output': output_str})}\n\n"

                    elif isinstance(data, ResponseOutputItemAddedEvent) and isinstance(
                        data.item, ResponseFunctionToolCall
                    ):
                        # Update the most recent call_id when a new tool call starts
                        most_recent_call_id = data.item.call_id
                        tool_call_names[most_recent_call_id] = data.item.name
                        yield f"data: {json.dumps({'type': 'tool_call_started', 'tool_name': data.item.name, 'call_id': most_recent_call_id})}\n\n"

                    elif isinstance(data, ResponseOutputItemDoneEvent) and isinstance(
                        data.item, ResponseFunctionToolCall
                    ):
                        call_id = data.item.call_id
                        yield f"data: {json.dumps({'type': 'tool_call_finished', 'tool_name': data.item.name, 'call_id': call_id})}\n\n"

                elif event.type == "run_item_stream_event":
                    item = event.item
                    if isinstance(item, ToolCallOutputItem):
                        output = item.raw_item["output"]

                        # Get the call_id and tool name directly
                        call_id = (
                            item.raw_item.get("call_id")
                            if hasattr(item, "raw_item")
                            else None
                        )
                        tool_name = tool_call_names.get(call_id)

                        print(
                            f"🔧 BACKEND: Tool response - call_id: {call_id}, tool_name: {tool_name}"
                        )

                        # Now yield with the correct call_id and tool name
                        if isinstance(output, str):
                            try:
                                parsed_output = json.loads(output)
                                yield f"data: {json.dumps({'type': 'tool_response', 'name': tool_name, 'call_id': call_id, 'output': parsed_output})}\n\n"
                            except Exception:
                                yield f"data: {json.dumps({'type': 'tool_response', 'name': tool_name, 'call_id': call_id, 'output': output})}\n\n"

                    # Add assistant response to history
                    if full_response:
                        # print(f"Full response: \n\n{full_response}")
                        chat_history.append(
                            {"role": "assistant", "content": full_response}
                        )

            # Signal completion
            yield f"data: {json.dumps({'type': 'completion', 'message': 'Response completed'})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': f'Agent execution error: {str(e)}'})}\n\n"

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
        "agent_name": session_data["agent"].name,
        "history_length": len(session_data["chat_history"]),
        "created_at": session_data["created_at"].isoformat(),
        "last_activity": session_data["last_activity"].isoformat(),
        "is_sse_active": session_id in active_sse_connections,
    }


@app.get("/sessions/{session_id}/tools")
async def list_session_tools(session_id: str):
    """List tools available for the agent in this session"""
    # Validate session
    if session_id not in agent_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session_data = agent_sessions[session_id]
    agent = session_data["agent"]
    mcp_servers = session_data["mcp_servers"]

    try:
        # Create run context
        run_context = {
            "session_id": session_id,
            "agent_name": agent.name,
            "conversation_id": session_id,  # Using session_id as conversation_id
        }

        # Get tools from all MCP servers
        tools_list = []
        for mcp_server in mcp_servers:
            tools = await mcp_server.list_tools(run_context=run_context, agent=agent)
            tools_list.extend(tools)

        # Format tools for response
        formatted_tools = []
        for tool in tools_list:
            # Extract tool information in a reasonable structure
            tool_info = {
                "name": tool.name,
                "description": tool.description,
            }

            # Add input schema if available
            if hasattr(tool, "input_schema") and tool.input_schema:
                tool_info["input_schema"] = tool.input_schema
            elif hasattr(tool, "inputSchema") and tool.inputSchema:
                tool_info["input_schema"] = tool.inputSchema

            formatted_tools.append(tool_info)

        return {
            "session_id": session_id,
            "agent_name": agent.name,
            "tools_count": len(formatted_tools),
            "tools": formatted_tools,
            "mcp_server_names": [mcp_server.name for mcp_server in mcp_servers],
        }

    except Exception as e:
        print(f"Error listing tools: {str(e)}")  # Add debug logging
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
