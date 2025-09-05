import './globals.css';
import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import { TooltipProvider } from "@/components/ui/tooltip";

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: 'DQA Agent',
  description: 'AI Agent Platform',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={inter.className}>
        <TooltipProvider>
          <div className="h-screen bg-[#f8f9fb]">
            {children}
          </div>
        </TooltipProvider>
      </body>
    </html>
  );
}
