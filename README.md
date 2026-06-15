# Distributed Systems in Go

Implementations of foundational distributed-systems primitives in Go, written as
self-directed coursework for **MIT 6.5840 / 6.824 (Distributed Systems)**.

The centerpiece is a from-scratch **Raft consensus** implementation — leader
election, log replication, persistence, and log compaction via snapshots —
written directly from the [Raft paper](https://raft.github.io/raft.pdf) (Figure 2
as the literal spec), with no lecture videos or reference implementations used.

## What's implemented

| Component | Path | What it does |
|---|---|---|
| **Raft consensus** | [`src/raft1/`](src/raft1/) | Replicated log via Raft: leader election, log replication, crash-recovery persistence, and snapshot-based log compaction (course labs 3A–3D). |
| **MapReduce** | [`src/mr/`](src/mr/) | Distributed MapReduce: a coordinator and workers communicating over RPC, with re-assignment of tasks when a worker crashes or stalls. |
| **Key/Value server** | [`src/kvsrv1/`](src/kvsrv1/) | Single-server linearizable key/value store with a retrying clerk and exactly-once `Put`/`Append` semantics over an unreliable network. |

> `src/kvraft1/` (fault-tolerant KV on Raft) and `src/shardkv1/` (sharded KV) are
> the **unmodified course skeletons** — next on the roadmap, not yet implemented.
> Everything else under `src/` (`labrpc`, `tester1`, `labgob`, `raftapi`, …) is
> MIT-provided test/RPC infrastructure that the solutions build against.

## Raft implementation notes

The interesting engineering lives in [`src/raft1/raft.go`](src/raft1/raft.go):

- **Figure 2, faithfully** — terms, `votedFor`, the commit rule (an entry is
  committed once it's on a majority *and* belongs to the current term), and the
  election restriction (§5.4.1) are all implemented to the letter.
- **Idempotent vote counting** — votes are tracked as a *set* of voters rather
  than a counter, so duplicated RPC deliveries can't manufacture a false majority.
- **Fast log backtracking** — `AppendEntries` conflict replies carry the
  conflicting term and its first index, so a lagging follower is reconciled in
  O(terms) round-trips instead of one-index-per-RPC.
- **Snapshots with a dual-index coordinate system** — the in-memory log operates
  on a trimmed tail; helpers translate between absolute log indices and physical
  slice offsets, and a lagging follower whose needed entries have been compacted
  away is caught up via the `InstallSnapshot` RPC.
- **Persistence** — Raft state and snapshot are saved together and restored on
  restart so a rebooted peer rejoins consistently.

### Testing & validation

```sh
cd src/raft1
go test                 # full 3A–3D suite
go test -run 3A         # a single sub-lab
go test -race           # race detector (needs a C toolchain on Windows)
```

Beyond the course suite, the implementation was stress-tested with many parallel
full-suite runs to shake out timing- and ordering-dependent bugs — e.g. 50×
Lab 3C and 32× full Lab 3D back-to-back, race-detector clean in the runs where
the toolchain was available.

## Running on Windows

Unusually, this repo runs on **Windows-native Go — no WSL required**. The 6.5840
harness assumes a Unix environment in places, so parts of the lab plumbing were
ported (including a TCP-less local-socket RPC path) to run directly on Windows.
*Those harness changes were AI-assisted; commit co-authorship reflects this.
The Raft, MapReduce, and KV implementations are mine.* PowerShell helpers wrap
the test suites:

```powershell
.\src\test-raft.ps1
.\src\test-mr.ps1
.\src\test-kv.ps1
```

The Go module targets Go 1.22 (`src/go.mod`); the only external dependency is
[`porcupine`](https://github.com/anishathalye/porcupine), the linearizability
checker the KV tests grade against.

## A note to current 6.5840 students

If you're taking 6.5840 (or any course that assigns Raft): please don't copy this.
The entire value of the lab is in implementing the algorithm yourself and debugging
your own race conditions — reading a finished solution skips precisely the part
that teaches you distributed systems. Read the paper, study Figure 2, and build it.

## References

- [Raft paper — *In Search of an Understandable Consensus Algorithm* (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf)
- [Raft visualization](https://raft.github.io/)
- [MapReduce paper (Dean & Ghemawat, 2004)](https://research.google/pubs/pub62/)
- [MIT 6.5840 course site](https://pdos.csail.mit.edu/6.5840/)
