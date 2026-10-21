import type { Event, Fault, NodeView } from './types'

const numerals = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX']

export function family(id: number): string {
  return `Family ${numerals[id - 1] ?? id}`
}

export function numeral(id: number): string {
  return numerals[id - 1] ?? String(id)
}

export function role(n: NodeView): string {
  if (!n.up) return 'silent'
  if (n.learner) return 'apprentice'
  if (!n.voter) return 'outside the Commission'
  switch (n.state) {
    case 'leader':
      return 'Don'
    case 'candidate':
      return 'claimant'
    case 'precandidate':
      return 'sounding out'
  }
  return 'sworn'
}

export const actionNames: Record<string, string> = {
  kill: 'kill the process',
  pause: 'freeze the process',
  isolate: 'cut it off',
  'stall-disk': 'slow its disk',
  'step-down': 'ask the Don to step down',
}

export const faultLabels: Record<string, string> = {
  kill: 'killed',
  pause: 'frozen',
  isolate: 'cut off',
  'stall-disk': 'slow disk',
  'step-down': 'stepped down',
}

export const realness: Record<string, string> = {
  kill: 'a real SIGKILL, and the process restarts when the fault ends',
  pause: 'a real SIGSTOP, then SIGCONT',
  isolate: 'in-process: the node drops every message to and from the others',
  'stall-disk': 'in-process: every fsync waits until the fault ends',
  'step-down': 'a leadership transfer to the next family',
}

export function describe(e: Event): string {
  const who = e.node ? family(e.node) : 'The Commission'
  switch (e.kind) {
    case 'succession':
      return `${who} leads reign ${e.term}`
    case 'down':
      return `${who} went silent`
    case 'up':
      return `${who} answers again`
    case 'membership':
      return `The Commission changed: ${e.text}`
    case 'fault':
      return `Chaos: ${e.text.replace(/node (\d+)/, (_, n) => family(Number(n)))}`
    case 'healed':
      return `${who} is whole again`
    case 'violation':
      return `A broken word: ${e.text}`
  }
}

export function outcome(f: Fault): string {
  const what = `${family(f.node)} was ${faultLabels[f.action] ?? f.action}`
  if (!f.on_leader) return `${what}. It was not the Don, so nobody had to wait.`
  if (!f.failover?.new_leader) return `${what}. The Don kept the seat.`
  const ms = (ns: number) => `${(ns / 1e6).toFixed(0)} ms`
  return `${what}. ${family(f.failover.new_leader)} took over after ${ms(f.failover.election_ns)}, and clients waited ${ms(f.failover.client_gap_ns)} between two acknowledged writes.`
}
