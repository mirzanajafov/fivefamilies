import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'

const root = process.env.RESULTS_DIR ?? path.resolve(process.cwd(), '..', 'bench', 'results')
const out = path.resolve(process.cwd(), 'src', 'generated', 'results.json')

const read = (...parts) => {
  const file = path.join(root, ...parts)
  return existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : undefined
}
const runs = (kind) =>
  existsSync(path.join(root, kind))
    ? readdirSync(path.join(root, kind), { withFileTypes: true })
        .filter((d) => d.isDirectory())
        .map((d) => d.name)
        .sort()
    : []
const files = (kind, run) => readdirSync(path.join(root, kind, run)).filter((f) => f.endsWith('.json'))
const laptopRuns = (kind) => runs(kind).filter((r) => r.endsWith('-laptop'))
const newestComplete = (kind, usable, among = runs(kind)) =>
  among.findLast((run) => {
    const names = files(kind, run)
    return names.length > 0 && names.every((name) => usable(read(kind, run, name)))
  })
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b)
  if (s.length === 0) return 0
  const mid = Math.floor(s.length / 2)
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2
}
const millis = (e) => (e.endsWith('ms') ? parseFloat(e) : parseFloat(e) * 1000)

function failover() {
  const run = newestComplete('e1', (f) => !f.error && f.trials?.some((t) => !t.error))
  if (!run) return null
  const rows = files('e1', run)
    .map((name) => {
      const f = read('e1', run, name)
      const ok = f.trials.filter((t) => !t.error)
      return {
        election: f.options.election,
        median: f.median_ns,
        min: f.min_ns,
        max: f.max_ns,
        electionMedian: median(ok.filter((t) => t.election_ns > 0).map((t) => t.election_ns)),
        trials: f.trials.length,
      }
    })
    .sort((a, b) => millis(a.election) - millis(b.election))
  return { run, rows }
}

function registry() {
  const run = newestComplete('registry', (f) => !f.error && Object.keys(f.variants ?? {}).length > 0)
  const f = run && read('registry', run, 'registry.json')
  return f ? { run, ttl: f.options.ttl_ns, election: f.options.election, variants: f.variants } : null
}

function lab() {
  return runs('lab').flatMap((run) => {
    const f = read('lab', run, 'report.json')
    if (!f) return []
    return [
      {
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
      },
    ]
  })
}

function point(step) {
  if (!step) return null
  const r = step.report
  const s = r.summary
  return {
    rate: step.rate,
    achieved: s.achieved_per_s,
    valid: s.valid && !step.error,
    kept: s.valid && !step.error && s.achieved_per_s >= 0.9 * step.rate && s.all.p99_ns <= 1e9 && s.all.errors <= 0.01 * s.scheduled,
    errors: s.all.errors,
    p50: s.all.p50_ns,
    p99: s.all.p99_ns,
    p999: s.all.p999_ns,
    readP50: s.reads.p50_ns,
    readP99: s.reads.p99_ns,
    writeP50: s.writes.p50_ns,
    writeP99: s.writes.p99_ns,
    fsyncsPerOp: r.fsyncs_per_op,
    msgsPerOp: r.msgs_per_op,
    entriesPerFsync: r.entries_per_fsync,
    elections: r.elections,
  }
}

function curves(kind, label) {
  const newest = new Map()
  for (const run of label ? [label] : laptopRuns(kind)) {
    const f = read(kind, run, `${kind}.json`)
    if (!f) continue
    const rounds = new Map()
    for (const c of f.map(curve)) rounds.set(c.name, [...(rounds.get(c.name) ?? []), c])
    for (const [name, cs] of rounds) newest.set(name, { ...combine(cs), run })
  }
  if (newest.size === 0) return null
  const all = [...newest.values()]
  return { run: [...new Set(all.map((c) => c.run))].join(', '), curves: all }
}

function curve(c) {
  const steps = c.steps
    .filter((s) => s.report.summary.scheduled > 0)
    .map(point)
    .sort((a, b) => a.rate - b.rate)
  const knee = c.knee_per_s
  const below = steps.filter((p) => p.rate <= 0.8 * knee).reverse()
  return {
    name: c.name,
    nodes: c.nodes,
    knee,
    latency: [point(c.at_80_percent), ...below, point(c.at_50_percent)].find((p) => p?.kept) ?? null,
    steps,
  }
}

function combine(cs) {
  const sorted = [...cs].sort((a, b) => a.knee - b.knee)
  const middle = sorted[Math.floor((sorted.length - 1) / 2)]
  return { ...middle, knee: median(cs.map((c) => c.knee)), rounds: cs.length, kneeMin: sorted[0].knee, kneeMax: sorted.at(-1).knee }
}

function batching() {
  const label = runs('e2')
    .filter((r) => r.endsWith('-batch-check'))
    .at(-1)
  const parts = label ? ['e2', 'e4'].map((k) => curves(k, label)).filter(Boolean) : []
  const names = {
    '5 nodes': 'writes, queue taken in one Ready',
    '5 nodes, one event per Ready': 'writes, one event per Ready',
    'index reads, 0.95': 'ReadIndex at 95% reads, queue taken in one Ready',
    'index reads, 0.95, one event per Ready': 'ReadIndex at 95% reads, one event per Ready',
  }
  return parts.length ? { run: label, curves: parts.flatMap((p) => p.curves).map((c) => ({ ...c, name: names[c.name] ?? c.name })) } : null
}

function e1b() {
  const run = newestComplete('e1b', (f) => !f.error && f.rows?.length > 0, laptopRuns('e1b'))
  const f = run && read('e1b', run, 'e1b.json')
  if (!f) return null
  return {
    run,
    rows: f.rows.map((r) => ({
      election: r.election_ns,
      sync: r.sync_storage,
      faults: r.faults.map((x) => ({ kind: x.kind, target: x.target, duration: x.duration_ns, injections: x.injections, elected: x.followed_by_an_election })),
    })),
  }
}

mkdirSync(path.dirname(out), { recursive: true })
writeFileSync(
  out,
  JSON.stringify(
    {
      failover: failover(),
      registry: registry(),
      lab: lab(),
      e1b: e1b(),
      e2: curves('e2'),
      e3: curves('e3'),
      e4: curves('e4'),
      e5: curves('e5'),
      batching: batching(),
    },
    null,
    2,
  ) + '\n',
)
