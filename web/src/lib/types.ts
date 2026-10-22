export type Role = '' | 'follower' | 'precandidate' | 'candidate' | 'leader'
export type FaultAction = 'kill' | 'pause' | 'isolate' | 'stall-disk' | 'step-down'

export type NodeView = {
  id: number
  up: boolean
  state: Role
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
  fault?: FaultAction
}

type EventBase = {
  at: string
  node?: number
  text: string
}

export type Event = (EventBase & { kind: 'succession'; term: number }) | (EventBase & { kind: 'down' | 'up' | 'membership' | 'fault' | 'healed' | 'violation' })

export type Window = {
  index: number
  from: string
  to: string
  ops: number
  result: 'Ok' | 'Illegal' | 'Unknown' | 'unchecked'
  took_ns: number
  note?: string
}

type Failover = {
  client_gap_ns: number
  election_ns: number
  new_leader: number
}

export type Fault = {
  id: number
  action: FaultAction
  node: number
  duration_ns: number
  started: string
  ends: string
  by: 'admin' | 'visitor'
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
