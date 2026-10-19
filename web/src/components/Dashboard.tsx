'use client'

import { useEffect, useState } from 'react'
import { useLive } from '@/lib/live'
import { rate } from '@/lib/format'
import { family, outcome } from '@/lib/words'
import { ChaosPanel } from './ChaosPanel'
import { Commission } from './Commission'
import { Families } from './Families'
import { Timeline, Windows } from './Ledger'

function Panel({ title, children, className = '' }: { title: string; children: React.ReactNode; className?: string }) {
  return (
    <section className={`panel p-4 sm:p-5 ${className}`}>
      <h2 className="serif mb-3 text-lg font-semibold">{title}</h2>
      {children}
    </section>
  )
}

export function Dashboard() {
  const { snapshot, connected } = useLive()
  const [admin, setAdmin] = useState(false)
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    fetch('/api/session')
      .then((r) => r.json())
      .then((s: { admin: boolean }) => setAdmin(s.admin))
      .catch(() => undefined)
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [])

  if (!snapshot) {
    return <p className="muted mt-10 text-center">{connected ? 'Listening to the Commission…' : 'Connecting to the Commission…'}</p>
  }
  const last = snapshot.last
  return (
    <div className="grid gap-4">
      <p className="muted max-w-3xl text-sm leading-relaxed">
        Five processes keep one replicated log with Raft, written from scratch in Go. The Don is the leader, and when it falls, the others
        hold a succession. Break a family below and watch the rest keep their word: every ten seconds the clients&apos; history is
        checked for linearizability.
      </p>
      {!connected && <p className="text-sm text-[var(--blood)]">The stream dropped and the page is reconnecting. What you see may be stale.</p>}
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.15fr)_minmax(0,1fr)]">
        <Panel title="The Commission">
          <Commission nodes={snapshot.nodes} leader={snapshot.leader} term={snapshot.term} />
          <div className="mt-4">
            <Families nodes={snapshot.nodes} leader={snapshot.leader} />
          </div>
          <p className="muted mt-4 text-xs">
            {snapshot.load.active
              ? `Demo clients: ${rate(snapshot.load.ops_per_s)} operations/s. ${snapshot.load.viewers} watching.`
              : 'The demo clients start when someone is watching.'}
          </p>
        </Panel>
        <div className="grid content-start gap-4">
          <Panel title="Chaos">
            <ChaosPanel snapshot={snapshot} admin={admin} />
          </Panel>
          <Panel title="Last fault">
            {last ? <p className="text-sm leading-relaxed">{outcome(last)}</p> : <p className="muted text-sm">Nothing has been broken yet.</p>}
            {last?.on_leader && last.failover?.new_leader ? (
              <dl className="mt-3 grid grid-cols-2 gap-3 text-center">
                <div className="rounded-lg border border-[var(--line)] p-2">
                  <dt className="muted text-xs">New Don after</dt>
                  <dd className="serif text-2xl tabular-nums">{(last.failover.election_ns / 1e6).toFixed(0)} ms</dd>
                </div>
                <div className="rounded-lg border border-[var(--line)] p-2">
                  <dt className="muted text-xs">Clients waited</dt>
                  <dd className="serif text-2xl tabular-nums">{(last.failover.client_gap_ns / 1e6).toFixed(0)} ms</dd>
                </div>
              </dl>
            ) : null}
          </Panel>
          <Panel title="Kept words">
            <Windows windows={snapshot.windows} />
          </Panel>
        </div>
      </div>
      <Panel title="The record">
        {snapshot.events.length > 0 ? <Timeline events={snapshot.events} now={now} /> : <p className="muted text-sm">Quiet so far.</p>}
        {snapshot.leader ? <p className="muted mt-3 text-xs">{family(snapshot.leader)} leads now.</p> : null}
      </Panel>
    </div>
  )
}
