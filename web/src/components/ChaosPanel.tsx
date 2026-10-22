'use client'

import { useState } from 'react'
import { errorIn } from '@/lib/api'
import { useNow } from '@/lib/clock'
import type { Snapshot } from '@/lib/types'
import { actionNames, family, faultActions, faultLabels, numeral, realness } from '@/lib/words'

export function ChaosPanel({ snapshot, admin }: { snapshot: Snapshot; admin: boolean }) {
  const [node, setNode] = useState(1)
  const [action, setAction] = useState<(typeof faultActions)[number]>('kill')
  const [seconds, setSeconds] = useState(5)
  const [busy, setBusy] = useState(false)
  const [refusal, setRefusal] = useState<{ text: string; during: string }>()
  const now = useNow(250)
  const max = admin ? 60 : snapshot.limits.max_seconds
  const fault = snapshot.fault
  const faultKey = [fault?.id, snapshot.last?.id].join()
  const message = refusal?.during === faultKey ? refusal.text : undefined
  const left = fault ? Math.max(0, Math.ceil((Date.parse(fault.ends) - now) / 1000)) : 0
  const cooldown = !admin && snapshot.limits.next_allowed ? Math.max(0, Math.ceil((Date.parse(snapshot.limits.next_allowed) - now) / 1000)) : 0
  const blocked = busy || !!fault || cooldown > 0

  async function post(path: string, body?: object) {
    setBusy(true)
    setRefusal(undefined)
    try {
      const res = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body && JSON.stringify(body) })
      if (!res.ok) setRefusal({ text: errorIn(await res.json().catch(() => undefined)) ?? `refused (${res.status})`, during: faultKey })
    } catch {
      setRefusal({ text: 'the dashboard could not reach the controller', during: faultKey })
    } finally {
      setBusy(false)
    }
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
          {faultActions.map((a) => (
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
          onClick={() => void post('/api/chaos', { action, node, seconds: Math.min(seconds, max) })}
          className="rounded-lg bg-[var(--blood)] px-4 py-2 text-sm font-medium text-white disabled:opacity-40"
        >
          {action === 'kill' ? 'Kill' : action === 'pause' ? 'Freeze' : action === 'isolate' ? 'Cut off' : 'Slow down'} {family(node)}
        </button>
        <button
          type="button"
          disabled={blocked || !snapshot.leader}
          onClick={() => void post('/api/chaos', { action: 'step-down' })}
          className="rounded-lg border border-[var(--line)] px-4 py-2 text-sm disabled:opacity-40"
        >
          Ask the Don to step down
        </button>
        {admin && (
          <button
            type="button"
            disabled={busy}
            onClick={() => void post('/api/heal')}
            className="rounded-lg border border-[var(--gold)] px-4 py-2 text-sm disabled:opacity-40"
          >
            Heal everything
          </button>
        )}
      </div>
      <p className="min-h-5 text-sm" role="status">
        {fault ? (
          <span className="text-[var(--blood)]">
            {family(fault.node)} is {faultLabels[fault.action]}
            {left > 0 ? ` for ${left} more s` : ', the Commission is settling'}
          </span>
        ) : cooldown > 0 ? (
          <span className="muted">Next fault in {cooldown} s</span>
        ) : (
          <span className="muted">One fault at a time, healed automatically.</span>
        )}
      </p>
      {message && (
        <p className="text-sm text-[var(--blood)]" role="alert">
          {message}
        </p>
      )}
    </div>
  )
}
