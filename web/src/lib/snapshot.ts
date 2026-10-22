import { actions, kinds, results, roles } from './types'
import type { Event, Fault, NodeView, Snapshot, Window } from './types'

type Fields = Record<string, unknown>

const isRecord = (v: unknown): v is Fields => typeof v === 'object' && v !== null && !Array.isArray(v)
const oneOf = (list: readonly string[], v: unknown) => typeof v === 'string' && list.includes(v)
const optional = (v: unknown, ok: (v: unknown) => boolean) => v === undefined || ok(v)
const all = (v: Fields, type: 'number' | 'string' | 'boolean', keys: readonly string[]) => keys.every((k) => typeof v[k] === type)

function isNode(v: unknown): v is NodeView {
  return (
    isRecord(v) &&
    all(v, 'number', ['id', 'term', 'leader', 'commit', 'last', 'msgs_per_s']) &&
    all(v, 'boolean', ['up', 'voter', 'learner']) &&
    oneOf(roles, v.state) &&
    optional(v.fault, (f) => oneOf(actions, f))
  )
}

function isEvent(v: unknown): v is Event {
  return (
    isRecord(v) &&
    all(v, 'string', ['at', 'text']) &&
    oneOf(kinds, v.kind) &&
    optional(v.node, (n) => typeof n === 'number') &&
    (v.kind !== 'succession' || typeof v.term === 'number')
  )
}

function isWindow(v: unknown): v is Window {
  return isRecord(v) && all(v, 'number', ['index', 'ops']) && oneOf(results, v.result) && optional(v.note, (n) => typeof n === 'string')
}

function isFault(v: unknown): v is Fault {
  return (
    isRecord(v) &&
    all(v, 'number', ['id', 'node']) &&
    typeof v.ends === 'string' &&
    typeof v.on_leader === 'boolean' &&
    oneOf(actions, v.action) &&
    optional(v.failover, (f) => isRecord(f) && all(f, 'number', ['client_gap_ns', 'election_ns', 'new_leader']))
  )
}

function isSnapshot(v: unknown): v is Snapshot {
  return (
    isRecord(v) &&
    typeof v.at === 'string' &&
    all(v, 'number', ['leader', 'term']) &&
    Array.isArray(v.nodes) &&
    v.nodes.every(isNode) &&
    Array.isArray(v.events) &&
    v.events.every(isEvent) &&
    Array.isArray(v.windows) &&
    v.windows.every(isWindow) &&
    optional(v.fault, isFault) &&
    optional(v.last, isFault) &&
    isRecord(v.load) &&
    typeof v.load.active === 'boolean' &&
    all(v.load, 'number', ['ops_per_s', 'viewers']) &&
    isRecord(v.limits) &&
    typeof v.limits.max_seconds === 'number' &&
    optional(v.limits.next_allowed, (t) => typeof t === 'string')
  )
}

export function snapshotIn(text: string): Snapshot | undefined {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    return undefined
  }
  return isSnapshot(parsed) ? parsed : undefined
}
