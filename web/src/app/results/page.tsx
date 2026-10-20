import type { Metadata } from 'next'
import { e1b, experiment, failover, lab, registry } from '@/lib/results'
import { ElectionCosts, ReadCurves, Table, WriteCurves } from '@/components/BenchTables'
import { LoadChart } from '@/components/LoadChart'

export const metadata: Metadata = { title: 'Results · FiveFamilies' }

const millis = (ns: number) => Math.round(ns / 1e6).toLocaleString('en-US')
const ms = (ns: number) => `${millis(ns)} ms`

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="panel min-w-0 p-4 sm:p-5">
      <h2 className="serif mb-3 text-lg font-semibold">{title}</h2>
      {children}
    </section>
  )
}

export default function Results() {
  const e1 = failover()
  const short = e1b()
  const [e2, e3, e4, e5, batched] = (['e2', 'e3', 'e4', 'e5', 'batching'] as const).map(experiment)
  const reg = registry()
  const runs = lab()
  const clean = runs.filter((r) => !r.bug)
  const planted = runs.filter((r) => r.bug)
  return (
    <div className="grid gap-4">
      <p className="muted max-w-3xl text-sm leading-relaxed">
        Numbers from the laptop (i7-1270P, Docker Desktop on WSL2, a Samsung PM9A1 NVMe), five nodes in containers, each result committed with the code that
        produced it. The design notes behind them, including what didn&apos;t work, are in the repository.
      </p>
      {e1 && (
        <Section title="How long a succession takes">
          <p className="mb-3 text-sm leading-relaxed">
            The Don is killed with SIGKILL under 200 writes a second, ten times per election timeout. The gap is the widest silence between two acknowledged
            writes, as a client sees it.
          </p>
          <Table
            head={['election timeout', 'clients waited, median', 'range', 'new Don chosen after']}
            rows={e1.rows.map((r) => [r.election, ms(r.median), `${millis(r.min)}–${ms(r.max)}`, ms(r.electionMedian)])}
          />
        </Section>
      )}
      {short && (
        <Section title="What a short election timeout costs">
          <p className="mb-3 text-sm leading-relaxed">
            No family is killed here. Under a steady 300 writes a second, a disk stall or a frozen event loop is forced on the Don or a follower five times
            each, and the table counts how many of those were followed by a succession that shouldn&apos;t have happened.
          </p>
          <ElectionCosts rows={short.rows} />
        </Section>
      )}
      {e2 && (
        <Section title="Throughput and latency">
          <p className="mb-3 text-sm leading-relaxed">
            Open-loop writes on a quiet cluster, doubling the rate until a step falls behind (under 90% of the offered rate, p99 over a second, or more than 1%
            failed), then two steps of bisection. The knee is the highest rate that kept up. Where a row ran three interleaved rounds, the knee is the median
            with the range. Latency comes from a run at 80% of the knee, or the nearest lower step that kept up.
          </p>
          <WriteCurves curves={e2.curves} />
          <div className="mt-4">
            <LoadChart
              curves={e2.curves.filter((c) => ['3 nodes', '5 nodes', '7 nodes', '5 nodes, no pipelining'].includes(c.name))}
              label="p99 latency against offered writes for three, five and seven nodes and five nodes without pipelining"
            />
          </div>
          <p className="mt-3 text-sm leading-relaxed">
            Three nodes stay ahead (7,000 to 8,000). Five and seven overlap, and the same five-node setup ranged from 4,000 to 7,000 over six rounds that
            evening. Taking the whole queue into one Ready halved the messages per write but didn&apos;t move the knee: one event per Ready reached 7,000 too.
            Without pipelining the appends wait for each other and carry dozens of entries, which reached 8,000 in its one round. Past the knee the cluster
            still collapses rather than slowing down, with or without the cap on pending writes.
          </p>
        </Section>
      )}
      {e3 && (
        <Section title="What durability costs">
          <p className="mb-3 text-sm leading-relaxed">
            The same five nodes with different fsync policies on the NVMe under WSL2. &quot;No fsync&quot; is unsafe and only here as a floor.
          </p>
          <WriteCurves curves={e3.curves} />
          <p className="mt-3 text-sm leading-relaxed">
            A flush takes about 9 ms on this disk, so one per write holds every node to about 110 writes a second. Group commit flushes about 40 entries at once
            near its knee. At low load the flush is nearly all of a write&apos;s latency: 13 ms with it, under a millisecond without. Writing on the leader
            before sending its appends reached 4,000 against 5,000 for writing in parallel, and added about 3 ms at low load (15.4 against 12.8 ms).
          </p>
        </Section>
      )}
      {e4 && (
        <Section title="Four ways to read">
          <p className="mb-3 text-sm leading-relaxed">
            Reads through the log, ReadIndex, the leader&apos;s lease and ReadIndex on followers, at 95% and 50% reads. The unsafe local read is a planted bug,
            here as a floor.
          </p>
          <ReadCurves curves={e4.curves} />
          <p className="mt-3 text-sm leading-relaxed">
            A lease read answers in a third of a millisecond and costs no messages until the leader is busy with writes. ReadIndex reached 12,000 in all three
            rounds against the lease&apos;s 16,000: reads that queue up share a heartbeat round, though below the knee one still costs about 14 messages,
            because there is rarely more than one waiting. ReadIndex on followers reached 20,000, more than the lease, because the reads themselves leave the
            leader. With half the operations writes, every path is limited by the writes.
          </p>
        </Section>
      )}
      {e5 && (
        <Section title="Next to etcd">
          <p className="mb-3 text-sm leading-relaxed">
            etcd 3.6.15 with the same timeouts, CPU pinning and load. etcd keeps a full MVCC history on disk and FiveFamilies keeps its state in memory, so
            FiveFamilies does less work per write.
          </p>
          <WriteCurves curves={e5.curves} unit="op" />
          <div className="mt-4">
            <LoadChart
              curves={e5.curves.filter((c) => c.name.includes('writes') && !c.name.includes('leader only'))}
              label="p99 latency against offered writes, FiveFamilies and etcd at three and five nodes"
            />
          </div>
          <p className="mt-3 text-sm leading-relaxed">
            etcd still wins on everything that goes through consensus: 2.7 times the writes on five nodes and 4 times on three, 2.7 times the linearizable
            reads. FiveFamilies&apos; unsafe local reads beat etcd&apos;s serializable ones (40,000 against 32,000), and at low load FiveFamilies writes faster
            (11 to 13 ms against 23 to 37 ms). Past their knees etcd slows down and keeps answering, but FiveFamilies falls over. etcd&apos;s five-node write
            median is a lower bound: in one round the step above it was ruled invalid because the load generator itself fell behind.
          </p>
        </Section>
      )}
      {batched && (
        <Section title="Taking the whole queue into one Ready">
          <p className="mb-3 text-sm leading-relaxed">
            After the runs above, the server loop takes everything queued, up to 256 events, before it asks Raft for a Ready. One event per Ready is the old
            behaviour, kept as a switch for this comparison, and both ran back to back on the same evening.
          </p>
          <WriteCurves curves={batched.curves} unit="op" />
          <p className="mt-3 text-sm leading-relaxed">
            ReadIndex gained the most: reads that queue up share a heartbeat round, the knee went from 8,000 to 14,000, and offered 16,000 a second the cluster
            still served 13,178 instead of 27. Writes send about half the messages (8 against 15 per write at 4,000 a second), but their knee moved by less than
            run-to-run noise, so something else holds writes back.
          </p>
        </Section>
      )}
      {e2 && (
        <Section title="Where the time goes">
          <p className="text-sm leading-relaxed">
            Go profiles of the Don under steady writes on five nodes. At 4,000 writes a second it used 54% of its CPU, and half of that went to write syscalls
            flushing gRPC streams, mostly to the followers. The loop with Raft, the store and the bookkeeping was 6%. At 6,000 a second it used 39%, with bigger
            batches and fewer flushes, and its request handlers spent their time waiting for commits, not for locks. So CPU time doesn&apos;t set the knee.
          </p>
          <p className="mt-3 text-sm leading-relaxed">
            Overloaded at 10,000 writes a second for 15 s, the cluster served 218 a second. With the Don refusing writes past 4,096 uncommitted entries it
            served 267, with 5,099 refusals and 85,629 timeouts: the cap bounds the queue inside Raft, but most writes waited in front of the event loop
            (refusing once that queue is full came later). After both overloads it went back to 4,000 a second cleanly.
          </p>
        </Section>
      )}
      {reg && (
        <Section title="How long a dead service stays listed">
          <p className="mb-3 text-sm leading-relaxed">
            An instance registered with a {reg.ttl / 1e9} s lease is killed, and a client watching the registry times how long it still sees it. A new Don gives
            every lease its full time again, so losing the Don as well adds an election.
          </p>
          <Table
            head={['what died', 'median', 'min', 'max', 'kills']}
            rows={Object.entries(reg.variants).map(([v, s]) => [
              v === 'instance' ? 'the instance' : 'the instance and the Don',
              ms(s.median_ns),
              ms(s.min_ns),
              ms(s.max_ns),
              s.n,
            ])}
          />
        </Section>
      )}
      {clean.length > 0 && (
        <Section title="Real faults, checked">
          <p className="mb-3 text-sm leading-relaxed">
            Kills, pauses, partitions, slow links, membership changes and leadership handovers on a real five-node cluster, with every client operation recorded
            and the whole history checked for linearizability afterwards.
          </p>
          <Table
            head={['run', 'faults', 'operations', 'linearizable', 'states equal']}
            rows={clean.map((r) => [
              r.run,
              `${r.faults} in ${r.seconds} s`,
              r.ops.toLocaleString('en-US'),
              r.linearizable === 'Ok' ? 'yes' : r.linearizable,
              r.statesEqual ? 'yes' : 'no',
            ])}
          />
        </Section>
      )}
      {planted.length > 0 && (
        <Section title="Bugs planted on purpose">
          <p className="mb-3 text-sm leading-relaxed">
            The same checks against builds with a known bug switched on, to show what the net catches on a real cluster and what it doesn&apos;t. The simulator
            catches every one of them. These runs were 90 seconds each.
          </p>
          <Table
            head={['bug', 'operations', 'linearizable', 'states equal', 'caught']}
            rows={planted.map((r) => [
              r.bug,
              r.ops.toLocaleString('en-US'),
              r.linearizable === 'Ok' ? 'yes' : r.linearizable,
              r.statesEqual ? 'yes' : 'no',
              r.linearizable !== 'Ok' || !r.statesEqual || r.problems.length > 0 ? 'yes' : 'not in 90 s',
            ])}
          />
        </Section>
      )}
    </div>
  )
}
