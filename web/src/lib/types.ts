export type NodeView = {
  id: number
  up: boolean
  state: string
  term: number
  leader: number
  commit: number
  applied: number
  last: number
  first: number
  snapshot: number
  voter: boolean
  learner: boolean
  msgs_per_s: number
  fault?: string
}

export type Event = {
  at: string
  kind: string
  node?: number
  term?: number
  text: string
}

export type Window = {
  index: number
  from: string
  to: string
  ops: number
  result: string
  took_ns: number
  note?: string
}

export type Failover = {
  client_gap_ns: number
  election_ns: number
  new_leader: number
}

export type Fault = {
  id: number
  action: string
  node: number
  duration_ns: number
  started: string
  ends: string
  by: string
  on_leader: boolean
  old_leader: number
  old_term: number
  failover?: Failover
}

export type Snapshot = {
  at: string
  nodes: NodeView[]
  leader: number
  term: number
  events: Event[]
  windows: Window[]
  fault?: Fault
  last?: Fault
  load: { active: boolean; ops_per_s: number; viewers: number }
  limits: { max_seconds: number; cooldown_seconds: number; next_allowed?: string }
}
