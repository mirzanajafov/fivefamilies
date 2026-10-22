import type { NodeView } from '@/lib/types'
import { rate } from '@/lib/format'
import { faultLabels, family, role } from '@/lib/words'

export function Families({ nodes, leader }: { nodes: NodeView[]; leader: number }) {
  const top = Math.max(1, ...nodes.map((n) => n.last))
  return (
    <div className="grid gap-2">
      {nodes.map((n) => {
        const isDon = n.id === leader && n.up
        return (
          <div key={n.id} className="grid grid-cols-[7.5rem_1fr] items-center gap-3 text-sm sm:grid-cols-[8rem_1fr_9rem]">
            <div>
              <div className={`font-medium ${isDon ? 'text-[var(--gold)]' : ''}`}>{family(n.id)}</div>
              <div className={`text-xs ${n.fault ? 'text-[var(--blood)]' : 'muted'}`}>{n.fault ? faultLabels[n.fault] : role(n)}</div>
            </div>
            <div aria-label={`log up to ${n.last}, committed ${n.commit}`}>
              <div className="relative h-2.5 overflow-hidden rounded-full bg-[var(--line)]">
                <div className="absolute inset-y-0 left-0 rounded-full bg-[var(--gold-soft)]" style={{ width: `${(100 * n.last) / top}%` }} />
                <div className="absolute inset-y-0 left-0 rounded-full bg-[var(--gold)]" style={{ width: `${(100 * n.commit) / top}%` }} />
              </div>
              <div className="muted mt-1 text-xs tabular-nums">{n.up ? `reign ${n.term} · log ${n.last} · committed ${n.commit}` : 'not answering'}</div>
            </div>
            <div className="muted col-span-2 text-xs tabular-nums sm:col-span-1 sm:text-right">{n.up ? `${rate(n.msgs_per_s)} messages/s` : ''}</div>
          </div>
        )
      })}
    </div>
  )
}
