export const roles = ['', 'follower', 'precandidate', 'candidate', 'leader'] as const
export const actions = ['kill', 'pause', 'isolate', 'stall-disk', 'step-down'] as const
export const results = ['Ok', 'Illegal', 'Unknown', 'unchecked'] as const
export const kinds = ['succession', 'down', 'up', 'membership', 'fault', 'healed', 'violation'] as const

export type Role = (typeof roles)[number]
export type FaultAction = (typeof actions)[number]

export type NodeView = {
  id: number
  up: boolean
  state: Role
  term: number
  commit: number
  last: number
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

export type Event = (EventBase & { kind: 'succession'; term: number }) | (EventBase & { kind: Exclude<(typeof kinds)[number], 'succession'> })

export type Window = {
  index: number
  ops: number
  result: (typeof results)[number]
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
  ends: string
  on_leader: boolean
  failover?: Failover
}

export type Snapshot = {
  nodes: NodeView[]
  leader: number
  term: number
  events: Event[]
  windows: Window[]
  fault?: Fault
  last?: Fault
  load: { active: boolean; ops_per_s: number; viewers: number }
  limits: { max_seconds: number; next_allowed?: string }
}
