import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'

const root = process.env.RESULTS_DIR ?? path.resolve(process.cwd(), '..', 'bench', 'results')
const out = path.resolve(process.cwd(), 'src', 'generated', 'results.json')

const read = (...parts) => {
  const file = path.join(root, ...parts)
  return existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : undefined
}
const runs = (kind) => (existsSync(path.join(root, kind)) ? readdirSync(path.join(root, kind)).sort() : [])
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b)
  if (s.length === 0) return 0
  const mid = Math.floor(s.length / 2)
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2
}
const millis = (e) => (e.endsWith('ms') ? parseFloat(e) : parseFloat(e) * 1000)

function failover() {
  const run = runs('e1').at(-1)
  if (!run) return null
  const rows = readdirSync(path.join(root, 'e1', run))
    .filter((f) => f.endsWith('.json'))
    .map((name) => {
      const f = read('e1', run, name)
      const ok = f.trials.filter((t) => !t.error)
      const gaps = ok.map((t) => t.unavailable_ns)
      return {
        election: f.options.election,
        median: median(gaps),
        min: Math.min(...gaps),
        max: Math.max(...gaps),
        electionMedian: median(ok.filter((t) => t.election_ns > 0).map((t) => t.election_ns)),
        trials: f.trials.length,
      }
    })
    .sort((a, b) => millis(a.election) - millis(b.election))
  return { run, rows }
}

function registry() {
  const run = runs('registry').at(-1)
  const f = run && read('registry', run, 'registry.json')
  return f ? { run, ttl: f.options.ttl_ns, election: f.options.election, variants: f.variants } : null
}

function lab() {
  return runs('lab').flatMap((run) => {
    const f = read('lab', run, 'report.json')
    if (!f) return []
    return [{
      run,
      seed: f.options.seed,
      seconds: f.options.duration_ns / 1e9,
      bug: f.options.bug || null,
      faults: f.faults?.length ?? 0,
      ops: f.ops,
      unknown: f.unknown_writes,
      linearizable: f.linearizable,
      statesEqual: f.states_equal,
      problems: f.problems ?? [],
    }]
  })
}

mkdirSync(path.dirname(out), { recursive: true })
writeFileSync(out, JSON.stringify({ failover: failover(), registry: registry(), lab: lab() }, null, 2) + '\n')
