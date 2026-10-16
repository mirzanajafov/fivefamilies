'use client'

import { useEffect, useState } from 'react'

export function AdminBox() {
  const [admin, setAdmin] = useState<boolean>()
  const [token, setToken] = useState('')
  const [error, setError] = useState<string>()
  useEffect(() => {
    fetch('/api/session')
      .then((r) => r.json())
      .then((s: { admin: boolean }) => setAdmin(s.admin))
      .catch(() => setAdmin(false))
  }, [])

  async function login(e: React.FormEvent) {
    e.preventDefault()
    setError(undefined)
    const res = await fetch('/api/session', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token }) })
    const body = (await res.json().catch(() => ({}))) as { admin?: boolean; error?: string }
    setToken('')
    if (!res.ok) {
      setError(body.error ?? 'refused')
      return
    }
    setAdmin(true)
  }

  async function logout() {
    await fetch('/api/session', { method: 'DELETE' })
    setAdmin(false)
  }

  return (
    <section className="panel p-5">
      <h1 className="serif mb-2 text-xl font-semibold">Admin</h1>
      {admin ? (
        <div className="grid gap-3 text-sm">
          <p>The limits are lifted in this browser: faults up to a minute, no cooldown, and a button to heal everything.</p>
          <button type="button" onClick={logout} className="justify-self-start rounded-lg border border-[var(--line)] px-4 py-2">
            Sign out
          </button>
        </div>
      ) : (
        <form onSubmit={login} className="grid gap-3 text-sm">
          <p className="muted">The admin token lifts the visitor limits. It stays in an httpOnly cookie on this site and is never readable by the page.</p>
          <input
            type="password"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            autoComplete="off"
            aria-label="admin token"
            className="rounded-lg border border-[var(--line)] bg-transparent px-3 py-2"
          />
          <button type="submit" disabled={!token} className="justify-self-start rounded-lg bg-[var(--gold)] px-4 py-2 font-medium text-black disabled:opacity-40">
            Open
          </button>
          {error && <p className="text-[var(--blood)]">{error}</p>}
        </form>
      )}
    </section>
  )
}
