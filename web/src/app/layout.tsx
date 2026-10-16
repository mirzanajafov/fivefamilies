import type { Metadata, Viewport } from 'next'
import Link from 'next/link'
import './globals.css'

export const metadata: Metadata = {
  title: 'FiveFamilies',
  description: 'Five nodes keep one log. Break them and watch the Commission recover.',
}

export const viewport: Viewport = {
  themeColor: [
    { media: '(prefers-color-scheme: light)', color: '#f4efe6' },
    { media: '(prefers-color-scheme: dark)', color: '#12100e' },
  ],
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen">
        <header className="mx-auto flex max-w-6xl items-center justify-between gap-4 px-4 pt-5 pb-3 sm:px-6">
          <Link href="/" className="flex items-baseline gap-2">
            <span className="serif text-xl font-semibold tracking-tight">FiveFamilies</span>
            <span className="muted hidden text-sm sm:inline">Raft, from scratch, under fire</span>
          </Link>
          <nav className="flex gap-4 text-sm">
            <Link href="/" className="muted hover:text-[var(--ink)]">
              Live
            </Link>
            <Link href="/results" className="muted hover:text-[var(--ink)]">
              Results
            </Link>
            <Link href="/admin" className="muted hover:text-[var(--ink)]">
              Admin
            </Link>
          </nav>
        </header>
        <main className="mx-auto max-w-6xl px-4 pb-16 sm:px-6">{children}</main>
      </body>
    </html>
  )
}
