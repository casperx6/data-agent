"use client";

import React, { useState, useRef, useEffect, useCallback } from "react";
import {
 Paperclip,
 ChevronRight,
} from "lucide-react";
import { useChatStore } from "@/lib/stores/chatStore";
import apiService from "@/lib/services/api";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";

import ToolList, { ToolListRef } from "./ToolList";

const ChatComposer = () => {
  const [input, setInput] = useState("");

  const [backendStatus, setBackendStatus] = useState<
    "connected" | "disconnected" | "checking"
  >("checking");

  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const {
    sessionId,
    isConnecting,
    sendMessage,
    isTyping,
  } = useChatStore();

  // Get refresh function from ToolList's polling hook
  const toolListRef = useRef<ToolListRef>(null);

  const isSessionActive = !!sessionId;

  // 调整文本框高度的函数
 const adjustTextareaHeight = useCallback(() => {
  const textarea = textareaRef.current;
  if (!textarea) return;

  // 重置高度以获取正确的scrollHeight
  textarea.style.height = 'auto';
   
  // 计算新高度
  const minHeight = 60;  // 对应 min-h-[60px]
  const maxHeight = 200; // 对应 max-h-[200px]
  const scrollHeight = textarea.scrollHeight;
  const newHeight = Math.min(Math.max(scrollHeight, minHeight), maxHeight);
   
  // 应用新高度
  textarea.style.height = `${newHeight}px`;
   
  // 调试信息（可选，生产环境可删除）
  console.log('Textarea height adjusted:', { scrollHeight, newHeight }); 
}, []);

  // 监听输入变化，自动调整高度
  useEffect(() => {
  adjustTextareaHeight(); 
}, [input, adjustTextareaHeight]);

 // 组件挂载后初始化高度
  useEffect(() => {
  adjustTextareaHeight();
}, [adjustTextareaHeight]);

  // Check backend connection on mount
  useEffect(() => {
    const checkBackendConnection = async () => {
      try {
        await apiService.healthCheck();
        setBackendStatus("connected");
      } catch {
        setBackendStatus("disconnected");
      }
    };

    checkBackendConnection();

    // Check every 30 seconds
    const interval = setInterval(checkBackendConnection, 30000);
    return () => clearInterval(interval);
  }, []);

  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
   setInput(e.target.value);
   // 立即调整高度，而不仅仅依赖useEffect
   setTimeout(adjustTextareaHeight, 0);
 };

  const handleSubmit = async (e: React.FormEvent) => {
   e.preventDefault();
   if (!input.trim() || isConnecting || isTyping) return;

   const userInput = input;
   setInput("");

   // 清空输入后立即调整高度
   setTimeout(adjustTextareaHeight, 0);

   // Send message using the simplified system
   const success = await sendMessage(userInput);
   
   if (!success) {
     console.error("Failed to send message");
   }
 };

  const handleToolClick = (toolName: string) => {
    setInput(`/${toolName} `);
    textareaRef.current?.focus();
  };

  const handleFileUpload = () => {
    fileInputRef.current?.click();
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  return (
    <div className="border-t bg-white h-full flex flex-col">
      <div className="w-3/4 mx-auto p-4 flex-1 flex flex-col overflow-hidden">
        {/* Input Form */}
        <form onSubmit={handleSubmit} className="space-y-4 flex-shrink-0">
          <div className="relative flex items-center space-x-2">
            <div className="flex-1 relative">
              <Textarea
                ref={textareaRef}
                value={input}
                onChange={handleInputChange}
                onKeyDown={handleKeyDown}
                placeholder={
                  isSessionActive 
                    ? "Type your message... (Press Enter to send, Shift+Enter for new line)"
                    : "Start a conversation..."
                }
                className="min-h-[60px] max-h-[200px] resize-none pr-12 text-sm"
                style={{
                  height: 'auto',
                  transition: 'height 0.1s ease' // 添加平滑过渡效果
               }}
                disabled={isConnecting || isTyping}
              />
              
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="absolute bottom-2 right-2 h-8 w-8"
                onClick={handleFileUpload}
                disabled={isConnecting || isTyping}
              >
                <Paperclip className="h-4 w-4" />
              </Button>
            </div>

            {/* Send Button - Now inline to the right */}
            <Button
              type="submit"
              disabled={
                !input.trim() || 
                isConnecting || 
                isTyping ||
                backendStatus !== "connected"
              }
              className="h-12 w-12 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 rounded-xl flex-shrink-0"
            >
              <ChevronRight className="h-5 w-5" />
            </Button>
          </div>

          <div className="flex items-center justify-between">
            <div className="text-xs text-gray-500">
              {/* Typing indicator removed */}
            </div>
          </div>
        </form>

        <input
          ref={fileInputRef}
          type="file"
          className="hidden"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) {
              setInput((prev) => prev + `\n[Attached: ${file.name}]`);
            }
          }}
        />

        {/* Dynamic Tool List - Now scrollable */}
        <div className="flex-1 mt-4 min-h-0 overflow-y-auto">
          <ToolList 
            onToolClick={handleToolClick} 
            ref={toolListRef}
            sessionId={sessionId}
          />
        </div>
      </div>
    </div>
  );
};

export default ChatComposer;
