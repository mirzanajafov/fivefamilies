import type { Event, Fault, FaultAction, NodeView, Role, Snapshot, Window } from './types'

const roles = ['', 'follower', 'precandidate', 'candidate', 'leader'] as const satisfies readonly Role[]
const actions = ['kill', 'pause', 'isolate', 'stall-disk', 'step-down'] as const satisfies readonly FaultAction[]
const results = ['Ok', 'Illegal', 'Unknown', 'unchecked'] as const satisfies readonly Window['result'][]
const kinds = ['succession', 'down', 'up', 'membership', 'fault', 'healed', 'violation'] as const satisfies readonly Event['kind'][]
const starters = ['admin', 'visitor'] as const satisfies readonly Fault['by'][]

type Fields = Record<string, unknown>

const isRecord = (v: unknown): v is Fields => typeof v === 'object' && v !== null && !Array.isArray(v)
const oneOf = (list: readonly string[], v: unknown) => typeof v === 'string' && list.includes(v)
const numbers = (v: Fields, keys: string[]) => keys.every((k) => typeof v[k] === 'number')
const strings = (v: Fields, keys: string[]) => keys.every((k) => typeof v[k] === 'string')
const optional = (v: unknown, ok: (v: unknown) => boolean) => v === undefined || ok(v)

function isNode(v: unknown): v is NodeView {
  return (
    isRecord(v) &&
    numbers(v, ['id', 'term', 'leader', 'commit', 'applied', 'last', 'first', 'snapshot', 'msgs_per_s']) &&
    typeof v.up === 'boolean' &&
    typeof v.voter === 'boolean' &&
    typeof v.learner === 'boolean' &&
    oneOf(roles, v.state) &&
    optional(v.fault, (f) => oneOf(actions, f))
  )
}

function isEvent(v: unknown): v is Event {
  return (
    isRecord(v) &&
    strings(v, ['at', 'text']) &&
    oneOf(kinds, v.kind) &&
    optional(v.node, (n) => typeof n === 'number') &&
    (v.kind !== 'succession' || typeof v.term === 'number')
  )
}

function isWindow(v: unknown): v is Window {
  return isRecord(v) && numbers(v, ['index', 'ops', 'took_ns']) && strings(v, ['from', 'to']) && oneOf(results, v.result)
}

function isFault(v: unknown): v is Fault {
  return (
    isRecord(v) &&
    numbers(v, ['id', 'node', 'duration_ns', 'old_leader', 'old_term']) &&
    strings(v, ['started', 'ends']) &&
    oneOf(actions, v.action) &&
    oneOf(starters, v.by) &&
    typeof v.on_leader === 'boolean' &&
    optional(v.failover, (f) => isRecord(f) && numbers(f, ['client_gap_ns', 'election_ns', 'new_leader']))
  )
}

function isSnapshot(v: unknown): v is Snapshot {
  return (
    isRecord(v) &&
    typeof v.at === 'string' &&
    numbers(v, ['leader', 'term']) &&
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
    numbers(v.load, ['ops_per_s', 'viewers']) &&
    isRecord(v.limits) &&
    numbers(v.limits, ['max_seconds', 'cooldown_seconds']) &&
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
