import type { Curve, E1bRow, Point } from '@/lib/results'

const ms = (ns: number) => (ns >= 10e6 ? `${Math.round(ns / 1e6).toLocaleString('en-US')} ms` : `${(ns / 1e6).toFixed(1)} ms`)
const per = (v: number) => (v === 0 ? '0' : v >= 10 ? v.toFixed(0) : v >= 1 ? v.toFixed(1) : v.toFixed(2))
const msgs = (v: number) => (v ? per(v) : 'not counted')
const rate = (v: number) => Math.round(v).toLocaleString('en-US')

export function Table({ head, rows }: { head: string[]; rows: React.ReactNode[][] }) {
  const first = head.length > 5 ? 'py-1.5 pr-4 min-w-[10rem]' : 'py-1.5 pr-4'
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
                <td key={j} className={j === 0 ? first : 'py-1.5 pr-4 whitespace-nowrap'}>
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

function latency(c: Curve, cells: (p: Point) => string[]): string[] {
  return c.latency ? [rate(c.latency.rate), ...cells(c.latency)] : Array<string>(5).fill('')
}

export function WriteCurves({ curves, unit = 'write' }: { curves: Curve[]; unit?: string }) {
  const cells = (p: Point) => [ms(p.p50), ms(p.p99), per(p.fsyncsPerOp), msgs(p.msgsPerOp)]
  return (
    <Table
      head={['configuration', `knee, ${unit}s/s`, 'latency taken at', 'p50', 'p99', `fsyncs per ${unit}`, `messages per ${unit}`]}
      rows={curves.map((c) => [c.name, c.knee ? rate(c.knee) : 'none', ...latency(c, cells)])}
    />
  )
}

export function ReadCurves({ curves }: { curves: Curve[] }) {
  const cells = (p: Point) => [ms(p.readP50), ms(p.readP99), ms(p.writeP99), msgs(p.msgsPerOp)]
  return (
    <Table
      head={['read path and share', 'knee, ops/s', 'latency taken at', 'read p50', 'read p99', 'write p99', 'messages per op']}
      rows={curves.map((c) => [c.name, c.knee ? rate(c.knee) : 'none', ...latency(c, cells)])}
    />
  )
}

export function ElectionCosts({ rows }: { rows: E1bRow[] }) {
  const plain = rows.filter((r) => !r.sync)
  const sync = rows.filter((r) => r.sync)
  const label = (f: E1bRow['faults'][number]) => `${f.kind === 'stall-disk' ? 'disk stall' : 'pause'} of ${ms(f.duration)} on the ${f.target}`
  const cell = (f?: E1bRow['faults'][number]) => (f ? `${f.elected} of ${f.injections}` : '')
  const faults = plain[0]?.faults ?? []
  const relative = (f: E1bRow['faults'][number]) => {
    const k = f.duration / plain[0].election
    return `pause of ${k === 0.5 ? 'T/2' : k === 1 ? 'T' : `${k}T`} on the ${f.target}`
  }
  return (
    <div className="grid gap-4">
      <Table
        head={['fault', ...plain.map((r) => `timeout ${ms(r.election)}`)]}
        rows={faults.map((f, i) => [f.kind === 'pause' ? relative(f) : label(f), ...plain.map((r) => cell(r.faults[i]))])}
      />
      {sync.length > 0 && (
        <Table
          head={['fault, storage waited on in the loop', ...sync.map((r) => `timeout ${ms(r.election)}`)]}
          rows={(sync[0]?.faults ?? []).map((f, i) => [label(f), ...sync.map((r) => cell(r.faults[i]))])}
        />
      )}
    </div>
  )
}
