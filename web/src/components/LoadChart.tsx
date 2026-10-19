import type { Curve, Point } from '@/lib/results'

const colors = ['var(--gold)', 'var(--blood)', 'var(--ok)', 'var(--ink)', 'var(--muted)']
const W = 400
const H = 240
const left = 46
const right = 10
const top = 10
const bottom = 34

const short = (v: number) => (v >= 1000 ? `${v / 1000}k` : `${Math.round(v)}`)
const latency = (ms: number) => (ms >= 1000 ? `${ms / 1000} s` : `${ms} ms`)

export function LoadChart({ curves, label }: { curves: Curve[]; label: string }) {
  const pts = curves.flatMap((c) => c.steps)
  if (pts.length === 0) return null
  const lx0 = Math.log2(Math.min(...pts.map((p) => p.rate)))
  const lx1 = Math.log2(Math.max(...pts.map((p) => p.rate)))
  const msOf = (p: Point) => (p.p99 > 0 ? p.p99 / 1e6 : Infinity)
  const finite = pts.map(msOf).filter(Number.isFinite)
  const ly0 = Math.floor(Math.log10(Math.max(0.1, Math.min(...finite))))
  const ly1 = Math.min(4, Math.ceil(Math.log10(Math.max(...finite, 1000))))
  const x = (rate: number) => left + 10 + ((Math.log2(rate) - lx0) / Math.max(1e-9, lx1 - lx0)) * (W - left - right - 20)
  const y = (ms: number) => top + (1 - (Math.min(Math.log10(ms), ly1) - ly0) / (ly1 - ly0)) * (H - top - bottom)
  const xticks: number[] = []
  for (let t = 500 * 2 ** Math.ceil(lx0 - Math.log2(500) - 1e-9); Math.log2(t) <= lx1 + 1e-9; t *= 2) xticks.push(t)
  const yticks: number[] = []
  for (let e = ly0; e <= ly1; e++) yticks.push(10 ** e)
  return (
    <figure className="grid max-w-lg gap-2">
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={label} className="w-full text-[11px]">
        {yticks.map((t) => (
          <g key={t}>
            <line x1={left} x2={W - right} y1={y(t)} y2={y(t)} stroke="var(--line)" />
            <text x={left - 6} y={y(t) + 4} textAnchor="end" fill="var(--muted)">
              {latency(t)}
            </text>
          </g>
        ))}
        {xticks.map((t) => (
          <text key={t} x={x(t)} y={H - bottom + 16} textAnchor="middle" fill="var(--muted)">
            {short(t)}
          </text>
        ))}
        <text x={(left + W - right) / 2} y={H - 4} textAnchor="middle" fill="var(--muted)">
          offered per second
        </text>
        {curves.map((c, i) => {
          const color = colors[i % colors.length]
          const line = c.steps.map((p) => `${x(p.rate).toFixed(1)},${y(msOf(p)).toFixed(1)}`).join(' ')
          return (
            <g key={c.name}>
              <polyline points={line} fill="none" stroke={color} strokeWidth={1.75} strokeDasharray={i >= colors.length ? '4 3' : undefined} />
              {c.steps.map((p) => (
                <circle key={p.rate} cx={x(p.rate)} cy={y(msOf(p))} r={2.75} fill={p.kept ? color : 'var(--panel)'} stroke={color} strokeWidth={1.25} />
              ))}
            </g>
          )
        })}
      </svg>
      <figcaption className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
        {curves.map((c, i) => (
          <span key={c.name} className="inline-flex items-center gap-1.5">
            <span className="inline-block h-0.5 w-4" style={{ background: colors[i % colors.length] }} />
            {c.name}
          </span>
        ))}
        <span className="muted">p99 latency (hollow points fell behind)</span>
      </figcaption>
    </figure>
  )
}
