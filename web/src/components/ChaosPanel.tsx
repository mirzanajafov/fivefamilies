'use client'

import { useEffect, useState } from 'react'
import type { Snapshot } from '@/lib/types'
import { actionNames, family, faultLabels, numeral, realness } from '@/lib/words'

const actions = ['kill', 'pause', 'isolate', 'slow-disk'] as const

export function ChaosPanel({ snapshot, admin }: { snapshot: Snapshot; admin: boolean }) {
  const [node, setNode] = useState(1)
  const [action, setAction] = useState<string>('kill')
  const [seconds, setSeconds] = useState(5)
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState<string>()
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 250)
    return () => clearInterval(t)
  }, [])
  const max = admin ? 60 : snapshot.limits.max_seconds
  const fault = snapshot.fault
  const left = fault ? Math.max(0, Math.ceil((Date.parse(fault.ends) - now) / 1000)) : 0
  const cooldown = !admin && snapshot.limits.next_allowed ? Math.max(0, Math.ceil((Date.parse(snapshot.limits.next_allowed) - now) / 1000)) : 0
  const blocked = busy || !!fault || cooldown > 0

  async function send(body: object) {
    setBusy(true)
    setMessage(undefined)
    try {
      const res = await fetch('/api/chaos', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      if (!res.ok) {
        const { error } = (await res.json().catch(() => ({}))) as { error?: string }
        setMessage(error ?? `refused (${res.status})`)
      }
    } catch {
      setMessage('the dashboard could not reach the controller')
    } finally {
      setBusy(false)
    }
  }

  async function heal() {
    setBusy(true)
    await fetch('/api/heal', { method: 'POST' }).catch(() => undefined)
    setBusy(false)
  }

  return (
    <div className="grid gap-4">
      <div>
        <div className="muted mb-2 text-xs tracking-wide uppercase">Target</div>
        <div className="flex flex-wrap gap-2">
          {snapshot.nodes.map((n) => (
            <button
              key={n.id}
              type="button"
              onClick={() => setNode(n.id)}
              aria-pressed={node === n.id}
              className={`serif min-w-11 rounded-lg border px-3 py-1.5 text-base ${node === n.id ? 'border-[var(--gold)] bg-[var(--gold-soft)]' : 'border-[var(--line)]'}`}
            >
              {numeral(n.id)}
              {n.id === snapshot.leader && <span className="ml-1 text-xs text-[var(--gold)]">Don</span>}
            </button>
          ))}
        </div>
      </div>
      <div>
        <div className="muted mb-2 text-xs tracking-wide uppercase">Fault</div>
        <div className="grid grid-cols-2 gap-2">
          {actions.map((a) => (
            <button
              key={a}
              type="button"
              onClick={() => setAction(a)}
              aria-pressed={action === a}
              className={`rounded-lg border px-3 py-2 text-left text-sm ${action === a ? 'border-[var(--gold)] bg-[var(--gold-soft)]' : 'border-[var(--line)]'}`}
            >
              {actionNames[a]}
            </button>
          ))}
        </div>
        <p className="muted mt-2 text-xs">{realness[action]}</p>
      </div>
      <label className="grid gap-1 text-sm">
        <span className="muted text-xs tracking-wide uppercase">
          For {seconds} s <span className="normal-case">(at most {max})</span>
        </span>
        <input
          type="range"
          min={1}
          max={max}
          value={Math.min(seconds, max)}
          onChange={(e) => setSeconds(Number(e.target.value))}
          className="accent-[var(--gold)]"
        />
      </label>
      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          disabled={blocked}
          onClick={() => void send({ action, node, seconds: Math.min(seconds, max) })}
          className="rounded-lg bg-[var(--blood)] px-4 py-2 text-sm font-medium text-white disabled:opacity-40"
        >
          {action === 'kill' ? 'Kill' : action === 'pause' ? 'Freeze' : action === 'isolate' ? 'Cut off' : 'Slow down'} {family(node)}
        </button>
        <button
          type="button"
          disabled={blocked || !snapshot.leader}
          onClick={() => void send({ action: 'step-down' })}
          className="rounded-lg border border-[var(--line)] px-4 py-2 text-sm disabled:opacity-40"
        >
          Ask the Don to step down
        </button>
        {admin && (
          <button
            type="button"
            disabled={busy}
            onClick={() => void heal()}
            className="rounded-lg border border-[var(--gold)] px-4 py-2 text-sm disabled:opacity-40"
          >
            Heal everything
          </button>
        )}
      </div>
      <p className="min-h-5 text-sm" role="status">
        {fault ? (
          <span className="text-[var(--blood)]">
            {family(fault.node)} is {faultLabels[fault.action] ?? fault.action}
            {left > 0 ? ` for ${left} more s` : ', the Commission is settling'}
          </span>
        ) : cooldown > 0 ? (
          <span className="muted">Next fault in {cooldown} s</span>
        ) : message ? (
          <span className="text-[var(--blood)]">{message}</span>
        ) : (
          <span className="muted">One fault at a time, healed automatically.</span>
        )}
      </p>
    </div>
  )
}
