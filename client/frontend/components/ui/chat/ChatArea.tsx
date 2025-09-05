"use client";

import React, { useEffect, useRef } from 'react';
import { useChatStore } from '@/lib/stores/chatStore';
import MessageBubble, { ToolCallBubble } from './MessageBubble';

const ChatArea = () => {
  const { messages, isTyping } = useChatStore();
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isTyping]);

  // Function to render messages and tool calls in chronological order
  const renderMessagesChronologically = () => {
    if (!messages) return null;

    const result: React.ReactNode[] = [];

    messages.forEach((message) => {
      // For user messages, just render the message bubble
      if (message.role === 'user') {
        result.push(<MessageBubble key={message.id} message={message} />);
        return;
      }

      // For assistant messages, implement true chronological ordering
      // The ideal flow is: tool call -> partial response -> tool call -> continued response
      
      const toolCalls = message.toolCalls || [];
      const hasContent = message.content.trim().length > 0;
      
      if (toolCalls.length === 0) {
        // No tool calls, just render the message
        if (hasContent || message.isStreaming) {
          result.push(<MessageBubble key={`${message.id}-content`} message={message} />);
        }
      } else if (toolCalls.length === 1) {
        // Single tool call - render tool call first, then response
        result.push(
          <ToolCallBubble key={`${message.id}-tool-${toolCalls[0].id}`} toolCall={toolCalls[0]} />
        );
        if (hasContent || message.isStreaming) {
          result.push(<MessageBubble key={`${message.id}-content`} message={message} />);
        }
      } else {
        // Multiple tool calls - for now, render all tool calls first, then response
        // This could be enhanced to interleave based on tool call completion timestamps
        toolCalls.forEach((toolCall, index) => {
          result.push(
            <ToolCallBubble key={`${message.id}-tool-${toolCall.id}`} toolCall={toolCall} />
          );
          
          // If this is the last tool call and we have content, render the message
          if (index === toolCalls.length - 1 && (hasContent || message.isStreaming)) {
            result.push(<MessageBubble key={`${message.id}-content`} message={message} />);
          }
        });
      }
    });

    return result;
  };

  return (
    <div 
      ref={scrollRef}
      className="h-full overflow-y-auto bg-gray-50 p-6"
    >
      <div className="w-3/4 mx-auto">
        {messages.length === 0 ? (
          <div className="h-full flex items-center justify-center">
            <div className="text-center max-w-md">
              <div className="w-16 h-16 bg-indigo-100 rounded-full flex items-center justify-center mb-4 mx-auto">
                <span className="text-2xl font-bold text-indigo-600">DQA</span>
              </div>
              <h2 className="text-xl font-semibold text-gray-900 mb-2">
                欢迎使用数据质量评估智能体
              </h2>
              <p className="text-gray-600 mb-6">
                随时准备使用可用的工具帮助您完成数据质量评估任务的数据质量评估智能体
              </p>
              <div className="space-y-2">
                <div className="bg-white rounded-lg p-3 text-sm text-left border border-gray-200">
                  <span className="font-medium">Try:</span> &quot;你有哪些工具&quot;
                </div>
                <div className="bg-white rounded-lg p-3 text-sm text-left border border-gray-200">
                  <span className="font-medium">Try:</span> &quot;你能做什么&quot;
                </div>
                <div className="bg-white rounded-lg p-3 text-sm text-left border border-gray-200">
                  <span className="font-medium">Try:</span> &quot;数据质量评估流程是什么&quot;
                </div>
              </div>
            </div>
          </div>
        ) : (
          <>
            {renderMessagesChronologically()}
          </>
        )}
      </div>
    </div>
  );
};

export default ChatArea;
