import type { Event, Window } from '@/lib/types'
import { ago } from '@/lib/format'
import { describe } from '@/lib/words'

export function Windows({ windows }: { windows: Window[] }) {
  if (windows.length === 0) {
    return <p className="muted text-sm">The first window is checked ten seconds after someone starts watching.</p>
  }
  const broken = windows.filter((w) => w.result === 'Illegal')
  const checked = windows.filter((w) => w.result === 'Ok').length
  return (
    <div className="grid gap-3">
      <div className="flex flex-wrap gap-1.5" aria-label="linearizability windows, oldest first">
        {windows.map((w) => (
          <span
            key={w.index}
            title={`window ${w.index}: ${w.ops} operations, ${w.result}${w.note ? ` (${w.note})` : ''}`}
            className={`inline-block h-4 w-4 rounded-sm ${w.result === 'Ok' ? 'bg-[var(--ok)]' : w.result === 'Illegal' ? 'bg-[var(--blood)]' : 'bg-[var(--line)]'}`}
          />
        ))}
      </div>
      <p className="text-sm">
        {broken.length > 0 ? (
          <span className="text-[var(--blood)]">Window {broken[0].index} is not linearizable: a word was broken.</span>
        ) : (
          <span>
            {checked} of the last {windows.length} windows checked, every one linearizable.
          </span>
        )}{' '}
        <span className="muted">Each covers ten seconds of the demo clients&apos; puts, reads and compare-and-swaps.</span>
      </p>
    </div>
  )
}

export function Timeline({ events, now }: { events: Event[]; now: number }) {
  const recent = [...events].reverse().slice(0, 30)
  return (
    <ol className="grid gap-1.5 text-sm">
      {recent.map((e, i) => (
        <li key={`${e.at}-${i}`} className="grid grid-cols-[4.5rem_1fr] gap-2">
          <span className="muted tabular-nums">{ago(e.at, now)}</span>
          <span className={e.kind === 'violation' || e.kind === 'fault' ? 'text-[var(--blood)]' : e.kind === 'succession' ? 'text-[var(--gold)]' : ''}>
            {describe(e)}
          </span>
        </li>
      ))}
    </ol>
  )
}
