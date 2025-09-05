"use client";
import React, { useState } from 'react';
import { useChatStore } from '@/lib/stores/chatStore';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import ConfirmModal from "@/components/ui/modal";

const ChatHeader = () => {
  const { clearMessages } = useChatStore();
  const [modalOpen, setModalOpen] = useState(false);

  const handleClearClick = () => setModalOpen(true);
  const handleConfirm = () => {
    clearMessages();
    setModalOpen(false);
  };
  const handleCancel = () => setModalOpen(false);

  return (
    <div className="h-16 bg-white border-b border-gray-200 flex items-center justify-between px-6">
      <div className="flex items-center space-x-4">
        <div className="flex items-center space-x-2">
          <h1 className="text-lg font-semibold text-gray-900">
            DQA Agent
          </h1>
          <Badge variant="secondary" className="bg-indigo-100 text-indigo-700">
            AI Assistant
          </Badge>
        </div>
      </div>

      <div className="flex items-center space-x-2">
        <Button
          variant="outline"
          size="sm"
          onClick={handleClearClick}
          className="text-gray-600 hover:text-gray-900"
        >
          Clear Chat
        </Button>
      </div>

      <ConfirmModal
        open={modalOpen}
        onConfirm={handleConfirm}
        onCancel={handleCancel}
      >
        确定要清除对话记录吗？
      </ConfirmModal>
    </div>
  );
};

export default ChatHeader;
