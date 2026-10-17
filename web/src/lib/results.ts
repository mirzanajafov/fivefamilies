import data from '@/generated/results.json'

export type FailoverRow = { election: string; median: number; min: number; max: number; electionMedian: number; trials: number }
export type RegistryVariant = { median_ns: number; min_ns: number; max_ns: number; n: number }
export type LabRun = {
  run: string
  seed: number
  seconds: number
  bug: string | null
  faults: number
  ops: number
  unknown: number
  linearizable: string
  statesEqual: boolean
  problems: string[]
}

export type Point = {
  rate: number
  achieved: number
  valid: boolean
  kept: boolean
  errors: number
  p50: number
  p99: number
  p999: number
  readP50: number
  readP99: number
  writeP50: number
  writeP99: number
  fsyncsPerOp: number
  msgsPerOp: number
  entriesPerFsync: number
  elections: number
}

export type Curve = {
  name: string
  nodes: number
  knee: number
  rounds: number
  kneeMin: number
  kneeMax: number
  run: string
  latency: Point | null
  steps: Point[]
}

export type Curves = { run: string; curves: Curve[] } | null

export type E1bRow = {
  election: number
  sync: boolean
  faults: { kind: string; target: string; duration: number; injections: number; elected: number }[]
}

type Results = {
  failover: { run: string; rows: FailoverRow[] } | null
  registry: { run: string; ttl: number; election: string; variants: Record<string, RegistryVariant> } | null
  lab: LabRun[]
  e1b?: { run: string; rows: E1bRow[] } | null
  e2?: Curves
  e3?: Curves
  e4?: Curves
  e5?: Curves
  batching?: Curves
}

const results = data as Results

export const failover = () => results.failover
export const registry = () => results.registry
export const lab = () => results.lab
export const e1b = () => results.e1b ?? null
export const experiment = (name: 'e2' | 'e3' | 'e4' | 'e5' | 'batching') => results[name] ?? null
