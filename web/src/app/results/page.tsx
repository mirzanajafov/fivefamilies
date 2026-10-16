import type { Metadata } from 'next'
import { failover, lab, registry } from '@/lib/results'

export const metadata: Metadata = { title: 'Results · FiveFamilies' }

const ms = (ns: number) => `${Math.round(ns / 1e6).toLocaleString('en-US')} ms`

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="panel min-w-0 p-4 sm:p-5">
      <h2 className="serif mb-3 text-lg font-semibold">{title}</h2>
      {children}
    </section>
  )
}

function Table({ head, rows }: { head: string[]; rows: React.ReactNode[][] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[28rem] text-sm tabular-nums">
        <thead>
          <tr className="muted border-b border-[var(--line)] text-left text-xs">
            {head.map((h) => (
              <th key={h} className="py-1.5 pr-4 font-normal">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-b border-[var(--line)] last:border-0">
              {r.map((c, j) => (
                <td key={j} className="py-1.5 pr-4">
                  {c}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function Results() {
  const e1 = failover()
  const reg = registry()
  const runs = lab()
  const clean = runs.filter((r) => !r.bug)
  const planted = runs.filter((r) => r.bug)
  return (
    <div className="grid gap-4">
      <p className="muted max-w-3xl text-sm leading-relaxed">
        Numbers from the laptop (i7-1270P, Docker Desktop on WSL2, a Samsung PM9A1 NVMe), five nodes in containers, each result
        committed with the code that produced it. The design notes behind them, including what didn&apos;t work, are in the repository.
      </p>
      {e1 && (
        <Section title="How long a succession takes">
          <p className="mb-3 text-sm leading-relaxed">
            The Don is killed with SIGKILL under 200 writes a second, ten times per election timeout. The gap is the widest silence
            between two acknowledged writes, as a client sees it.
          </p>
          <Table
            head={['election timeout', 'clients waited, median', 'range', 'new Don chosen after']}
            rows={e1.rows.map((r) => [r.election, ms(r.median), `${ms(r.min)} – ${ms(r.max)}`, ms(r.electionMedian)])}
          />
        </Section>
      )}
      {reg && (
        <Section title="How long a dead service stays listed">
          <p className="mb-3 text-sm leading-relaxed">
            An instance registered with a {reg.ttl / 1e9} s lease is killed, and a client watching the registry times how long it
            still sees it. A new Don gives every lease its full time again, so losing the Don as well adds an election.
          </p>
          <Table
            head={['what died', 'median', 'min', 'max', 'kills']}
            rows={Object.entries(reg.variants).map(([v, s]) => [v === 'instance' ? 'the instance' : 'the instance and the Don', ms(s.median_ns), ms(s.min_ns), ms(s.max_ns), s.n])}
          />
        </Section>
      )}
      {clean.length > 0 && (
        <Section title="Real faults, checked">
          <p className="mb-3 text-sm leading-relaxed">
            Kills, pauses, partitions, slow links, membership changes and leadership handovers on a real five-node cluster, with every
            client operation recorded and the whole history checked for linearizability afterwards.
          </p>
          <Table
            head={['run', 'faults', 'operations', 'linearizable', 'states equal']}
            rows={clean.map((r) => [r.run, `${r.faults} in ${r.seconds} s`, r.ops.toLocaleString('en-US'), r.linearizable === 'Ok' ? 'yes' : r.linearizable, r.statesEqual ? 'yes' : 'no'])}
          />
        </Section>
      )}
      {planted.length > 0 && (
        <Section title="Bugs planted on purpose">
          <p className="mb-3 text-sm leading-relaxed">
            The same checks against builds with a known bug switched on, to show what the net catches on a real cluster and what it
            doesn&apos;t. The simulator catches every one of them; these runs were 90 seconds each.
          </p>
          <Table
            head={['bug', 'operations', 'linearizable', 'states equal', 'caught']}
            rows={planted.map((r) => [r.bug, r.ops.toLocaleString('en-US'), r.linearizable === 'Ok' ? 'yes' : r.linearizable, r.statesEqual ? 'yes' : 'no', r.linearizable !== 'Ok' || !r.statesEqual || r.problems.length > 0 ? 'yes' : 'not in 90 s'])}
          />
        </Section>
      )}
    </div>
  )
}
