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

type Results = {
  failover: { run: string; rows: FailoverRow[] } | null
  registry: { run: string; ttl: number; election: string; variants: Record<string, RegistryVariant> } | null
  lab: LabRun[]
}

const results = data as Results

export const failover = () => results.failover
export const registry = () => results.registry
export const lab = () => results.lab
