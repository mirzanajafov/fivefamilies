'use client'

import { useState } from 'react'
import { errorIn } from '@/lib/api'
import { useAdmin } from '@/lib/session'

export function AdminBox() {
  const [admin, setAdmin] = useAdmin()
  const [token, setToken] = useState('')
  const [error, setError] = useState<string>()

  async function session(init: RequestInit): Promise<boolean> {
    setError(undefined)
    try {
      const res = await fetch('/api/session', init)
      if (res.ok) return true
      setError(errorIn(await res.json().catch(() => undefined)) ?? `refused (${res.status})`)
    } catch {
      setError('the dashboard could not reach its server')
    }
    return false
  }

  async function login(e: React.SubmitEvent<HTMLFormElement>) {
    e.preventDefault()
    const ok = await session({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token }) })
    setToken('')
    if (ok) setAdmin(true)
  }

  async function logout() {
    if (await session({ method: 'DELETE' })) setAdmin(false)
  }

  return (
    <section className="panel p-5">
      <h1 className="serif mb-2 text-xl font-semibold">Admin</h1>
      {admin ? (
        <div className="grid gap-3 text-sm">
          <p>The limits are lifted in this browser: faults up to a minute, no cooldown, and a button to heal everything.</p>
          <button type="button" onClick={() => void logout()} className="justify-self-start rounded-lg border border-[var(--line)] px-4 py-2">
            Sign out
          </button>
        </div>
      ) : (
        <form onSubmit={(e) => void login(e)} className="grid gap-3 text-sm">
          <p className="muted">The admin token lifts the visitor limits. It stays in an httpOnly cookie on this site and is never readable by the page.</p>
          <input
            type="password"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            autoComplete="off"
            aria-label="admin token"
            className="rounded-lg border border-[var(--line)] bg-transparent px-3 py-2"
          />
          <button
            type="submit"
            disabled={!token}
            className="justify-self-start rounded-lg bg-[var(--gold)] px-4 py-2 font-medium text-black disabled:opacity-40"
          >
            Open
          </button>
        </form>
      )}
      {error && (
        <p className="mt-3 text-sm text-[var(--blood)]" role="alert">
          {error}
        </p>
      )}
    </section>
  )
}
