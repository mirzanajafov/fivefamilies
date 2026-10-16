import { describe as suite, expect, it } from 'vitest'
import { ago, rate } from './format'
import { describe, family, outcome, role } from './words'
import type { Fault, NodeView } from './types'

const node = (over: Partial<NodeView>): NodeView => ({
  id: 1, up: true, state: 'follower', term: 3, leader: 2, commit: 10, applied: 10, last: 10, first: 1, snapshot: 0, voter: true, learner: false, msgs_per_s: 0, ...over,
})

suite('words', () => {
  it('names families with numerals', () => {
    expect(family(4)).toBe('Family IV')
  })
  it('gives each node its role in the Commission', () => {
    expect(role(node({ state: 'leader' }))).toBe('Don')
    expect(role(node({}))).toBe('sworn')
    expect(role(node({ up: false, state: 'leader' }))).toBe('silent')
    expect(role(node({ learner: true, voter: false }))).toBe('apprentice')
    expect(role(node({ voter: false }))).toBe('outside the Commission')
  })
  it('tells a succession and a fault in its own words', () => {
    expect(describe({ at: '', kind: 'succession', node: 3, term: 7, text: 'node 3 leads term 7' })).toBe('Family III leads reign 7')
    expect(describe({ at: '', kind: 'fault', node: 2, text: 'kill on node 2 for 3s (visitor)' })).toBe('Chaos: kill on Family II for 3s (visitor)')
  })
  it('sums up a failover with both numbers', () => {
    const f: Fault = {
      id: 1, action: 'kill', node: 3, duration_ns: 3e9, started: '', ends: '', by: 'visitor', on_leader: true, old_leader: 3, old_term: 6,
      failover: { election_ns: 1.28e9, client_gap_ns: 1.81e9, new_leader: 4 },
    }
    expect(outcome(f)).toBe('Family III was killed. Family IV took over after 1280 ms; clients waited 1810 ms between two acknowledged writes.')
    expect(outcome({ ...f, on_leader: false })).toBe('Family III was killed. It was not the Don, so nobody had to wait.')
  })
})

suite('format', () => {
  it('rounds rates and ages', () => {
    expect(rate(19.93)).toBe('19.9')
    expect(rate(250.4)).toBe('250')
    expect(ago(new Date(1000).toISOString(), 1000 + 90_000)).toBe('1m ago')
  })
})
