import type { Metadata } from 'next'
import { e1b, experiment, failover, lab, registry } from '@/lib/results'
import { ElectionCosts, ReadCurves, Table, WriteCurves } from '@/components/BenchTables'
import { LoadChart } from '@/components/LoadChart'

export const metadata: Metadata = { title: 'Results · FiveFamilies' }

const ms = (ns: number) => `${Math.round(ns / 1e6).toLocaleString('en-US')} ms`

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
  const [e2, e3, e4, e5] = (['e2', 'e3', 'e4', 'e5'] as const).map(experiment)
  const reg = registry()
  const runs = lab()
  const clean = runs.filter((r) => !r.bug)
  const planted = runs.filter((r) => r.bug)
  return (
    <div className="grid gap-4">
      <p className="muted max-w-3xl text-sm leading-relaxed">
        Numbers from the laptop (i7-1270P, Docker Desktop on WSL2, a Samsung PM9A1 NVMe), five nodes in containers, each result
        committed with the code that produced it. The design notes behind them, including what didn&apos;t work, are in the repository.
      </p>
      {e1 && (
        <Section title="How long a succession takes">
          <p className="mb-3 text-sm leading-relaxed">
            The Don is killed with SIGKILL under 200 writes a second, ten times per election timeout. The gap is the widest silence
            between two acknowledged writes, as a client sees it.
          </p>
          <Table
            head={['election timeout', 'clients waited, median', 'range', 'new Don chosen after']}
            rows={e1.rows.map((r) => [r.election, ms(r.median), `${ms(r.min)} – ${ms(r.max)}`, ms(r.electionMedian)])}
          />
        </Section>
      )}
      {short && (
        <Section title="What a short election timeout costs">
          <p className="mb-3 text-sm leading-relaxed">
            No family is killed here. Under a steady 300 writes a second, a disk stall or a frozen event loop is forced on the Don
            or a follower five times each, and the table counts how many of those were followed by a succession that shouldn&apos;t
            have happened.
          </p>
          <ElectionCosts rows={short.rows} />
        </Section>
      )}
      {e2 && (
        <Section title="Throughput and latency">
          <p className="mb-3 text-sm leading-relaxed">
            Open-loop writes on a quiet cluster, doubling the rate until a step falls behind (under 90% of the offered rate, p99
            over a second, or more than 1% failed), then two steps of bisection. The knee is the highest rate that kept up;
            latency comes from a run at 80% of it, or the nearest lower step that kept up.
          </p>
          <WriteCurves curves={e2.curves} />
          <div className="mt-4">
            <LoadChart curves={e2.curves.filter((c) => !c.name.includes('append') && !c.name.includes('KiB'))} label="p99 latency against offered writes for three, five and seven nodes and five nodes without pipelining" />
          </div>
          <p className="mt-3 text-sm leading-relaxed">
            Cluster size moved the knee less than chance does: the same five nodes reached 4,000 here and 6,000 in the etcd
            comparison an hour later. The number that stands out is messages per write. With pipelining each write goes to every
            follower as its own append, because the event loop hands Raft one event at a time; without pipelining the appends wait
            for each other and carry dozens of entries, and the knee doubled. Past the knee the cluster doesn&apos;t level off, it
            collapses: writes time out and the leader keeps working on requests nobody is waiting for.
          </p>
        </Section>
      )}
      {e3 && (
        <Section title="What durability costs">
          <p className="mb-3 text-sm leading-relaxed">
            The same five nodes with different fsync policies on the NVMe under WSL2. &quot;No fsync&quot; is unsafe and only here as a
            floor.
          </p>
          <WriteCurves curves={e3.curves} />
          <p className="mt-3 text-sm leading-relaxed">
            A flush takes about 9 ms on this disk, so one per write holds every node to about 110 writes a second. Group commit
            flushes about 40 entries at once near its knee. At low load the flush is nearly all of a write&apos;s latency: 13 ms with
            it, under a millisecond without. Writing on the leader before sending added 2 to 3 ms at low load; its knee is not a fair
            number yet, because that setting also holds back the heartbeats queued with the entries, and it caused two elections.
          </p>
        </Section>
      )}
      {e4 && (
        <Section title="Four ways to read">
          <p className="mb-3 text-sm leading-relaxed">
            Reads through the log, ReadIndex, the leader&apos;s lease and ReadIndex on followers, at 95% and 50% reads. The unsafe
            local read is a planted bug, here as a floor.
          </p>
          <ReadCurves curves={e4.curves} />
          <p className="mt-3 text-sm leading-relaxed">
            A lease read costs no messages and answers in about half a millisecond until the leader is busy with writes. ReadIndex
            should cost one heartbeat round shared by every read waiting for it; here it costs 10 to 16 messages per operation, a
            round per read, for the same reason writes aren&apos;t batched. That is why it reaches half of the lease&apos;s throughput, and
            follower reads, which add a round trip to the leader, don&apos;t raise it much.
          </p>
        </Section>
      )}
      {e5 && (
        <Section title="Next to etcd">
          <p className="mb-3 text-sm leading-relaxed">
            etcd 3.6.15 with the same timeouts, CPU pinning and load. etcd keeps a full MVCC history on disk and FiveFamilies keeps
            its state in memory, so FiveFamilies does less work per write.
          </p>
          <WriteCurves curves={e5.curves} unit="op" />
          <div className="mt-4">
            <LoadChart curves={e5.curves.filter((c) => c.name.includes('writes') && !c.name.includes('leader only'))} label="p99 latency against offered writes, FiveFamilies and etcd at three and five nodes" />
          </div>
          <p className="mt-3 text-sm leading-relaxed">
            etcd wins clearly anyway: about three times the writes and four times the linearizable reads. At low load
            FiveFamilies is faster (13 ms against 22 to 38 ms for a write), and past their knees they behave differently: etcd
            slows down and keeps answering, FiveFamilies falls over. Both point at the same thing, batching under load, which is
            the next change to make and measure. etcd&apos;s five-node write knee is a lower bound: the step above it was ruled
            invalid because the load generator itself fell behind.
          </p>
        </Section>
      )}
      {reg && (
        <Section title="How long a dead service stays listed">
          <p className="mb-3 text-sm leading-relaxed">
            An instance registered with a {reg.ttl / 1e9} s lease is killed, and a client watching the registry times how long it
            still sees it. A new Don gives every lease its full time again, so losing the Don as well adds an election.
          </p>
          <Table
            head={['what died', 'median', 'min', 'max', 'kills']}
            rows={Object.entries(reg.variants).map(([v, s]) => [v === 'instance' ? 'the instance' : 'the instance and the Don', ms(s.median_ns), ms(s.min_ns), ms(s.max_ns), s.n])}
          />
        </Section>
      )}
      {clean.length > 0 && (
        <Section title="Real faults, checked">
          <p className="mb-3 text-sm leading-relaxed">
            Kills, pauses, partitions, slow links, membership changes and leadership handovers on a real five-node cluster, with every
            client operation recorded and the whole history checked for linearizability afterwards.
          </p>
          <Table
            head={['run', 'faults', 'operations', 'linearizable', 'states equal']}
            rows={clean.map((r) => [r.run, `${r.faults} in ${r.seconds} s`, r.ops.toLocaleString('en-US'), r.linearizable === 'Ok' ? 'yes' : r.linearizable, r.statesEqual ? 'yes' : 'no'])}
          />
        </Section>
      )}
      {planted.length > 0 && (
        <Section title="Bugs planted on purpose">
          <p className="mb-3 text-sm leading-relaxed">
            The same checks against builds with a known bug switched on, to show what the net catches on a real cluster and what it
            doesn&apos;t. The simulator catches every one of them; these runs were 90 seconds each.
          </p>
          <Table
            head={['bug', 'operations', 'linearizable', 'states equal', 'caught']}
            rows={planted.map((r) => [r.bug, r.ops.toLocaleString('en-US'), r.linearizable === 'Ok' ? 'yes' : r.linearizable, r.statesEqual ? 'yes' : 'no', r.linearizable !== 'Ok' || !r.statesEqual || r.problems.length > 0 ? 'yes' : 'not in 90 s'])}
          />
        </Section>
      )}
    </div>
  )
}
