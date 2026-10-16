import type { NodeView } from '@/lib/types'
import { faultLabels, numeral, role } from '@/lib/words'

const size = 420
const center = size / 2
const radius = 150
const seat = 40

function position(i: number, n: number) {
  const angle = -Math.PI / 2 + (2 * Math.PI * i) / n
  return { x: center + radius * Math.cos(angle), y: center + radius * Math.sin(angle) }
}

export function Commission({ nodes, leader, term }: { nodes: NodeView[]; leader: number; term: number }) {
  const seats = nodes.map((n, i) => ({ n, ...position(i, nodes.length) }))
  const don = seats.find((s) => s.n.id === leader && s.n.up)
  const busiest = Math.max(1, ...nodes.map((n) => n.msgs_per_s))
  return (
    <svg viewBox={`0 0 ${size} ${size}`} className="mx-auto block w-full max-w-[460px]" role="img" aria-label={`The Commission: ${nodes.length} families, ${leader ? `family ${numeral(leader)} leads reign ${term}` : 'no Don'}`}>
      {don &&
        seats
          .filter((s) => s !== don && s.n.up)
          .map((s) => (
            <line
              key={`line-${s.n.id}`}
              x1={don.x}
              y1={don.y}
              x2={s.x}
              y2={s.y}
              stroke="var(--gold)"
              strokeOpacity={0.55}
              strokeWidth={1 + 3 * Math.min(1, don.n.msgs_per_s / busiest)}
              className={don.n.msgs_per_s > 0 ? 'flow' : undefined}
            />
          ))}
      <circle cx={center} cy={center} r={66} fill="var(--panel)" stroke="var(--line)" />
      <text x={center} y={center - 8} textAnchor="middle" className="serif" fontSize="15" fill="var(--muted)">
        {don ? 'Reign' : leader ? 'Succession' : 'No Don'}
      </text>
      <text x={center} y={center + 22} textAnchor="middle" className="serif" fontSize="30" fontWeight="600" fill={don ? 'var(--ink)' : 'var(--muted)'}>
        {leader ? term : '—'}
      </text>
      {seats.map(({ n, x, y }) => {
        const isDon = n.id === leader && n.up
        const label = n.fault ? faultLabels[n.fault] ?? n.fault : role(n)
        return (
          <g key={n.id}>
            <circle
              cx={x}
              cy={y}
              r={seat}
              fill={n.up ? 'var(--seat)' : 'var(--silent)'}
              stroke={isDon ? 'var(--gold)' : n.fault ? 'var(--blood)' : 'var(--line)'}
              strokeWidth={isDon ? 4 : 2}
              strokeDasharray={n.fault && !isDon ? '5 4' : undefined}
            />
            <text x={x} y={y + 9} textAnchor="middle" className="serif" fontSize="26" fontWeight="600" fill={n.up ? 'var(--ink)' : 'var(--muted)'}>
              {numeral(n.id)}
            </text>
            <text
              x={x}
              y={y + seat + 18}
              textAnchor="middle"
              fontSize="13"
              fill={n.fault ? 'var(--blood)' : isDon ? 'var(--gold)' : 'var(--muted)'}
              fontWeight={isDon ? 600 : 400}
              stroke="var(--panel)"
              strokeWidth={5}
              paintOrder="stroke"
            >
              {label}
            </text>
          </g>
        )
      })}
    </svg>
  )
}
