package flow

/** What a fragment of dataflow *requires* of the topology it lands in.
  *
  * Rama enforces two rules at module declaration that the untyped API states nowhere:
  *
  *  - **Writes need an owning ETL topology.** "A PState can only be updated by the ETL
  *    topology that owns it" — and a query topology owns none. Rama's `Topologies.query`
  *    returns a bare `Block$Out` with no `pstate` method, where `stream`/`microbatch`
  *    return an `ETLTopologyBase` that has one.
  *  - **A value-producing `agg` needs a batch block.** "Aggregators can emit values
  *    rather than write into PStates in any batch block", and "microbatch topologies can
  *    use batch blocks through the `batchBlock` method, query topologies are implicitly
  *    batch blocks, and **stream topologies do not support them**."
  *
  * So [[Flow]] carries a second parameter naming the capabilities it needs, and it is
  * **contravariant**: `Flow[A, C]` reads as "runs in any context providing `C`".
  *
  *  - A fragment needing nothing is `Flow[A, Any]`
  *  - Composition intersects: binding `Flow[A, C1]` with `Flow[B, C2]` gives
  *    `Flow[B, C1 & C2]` — "needs both".
  *  - Accepting is one subtype check. `query` takes `Flow[R, QueryContext]`, and
  *    contravariance turns that into `QueryContext <: C`. Since `QueryContext` extends
  *    [[Batch]] but not [[Etl]], a flow needing `Batch` fits and one needing `Etl & Batch`
  *    does not.
  *
  */
object Context

/** Requires a batch block: a value-producing `agg`, as [[Flow.aggList]] compiles to.
  *
  * Available in query topologies (implicitly batch blocks) and inside a microbatch
  * topology's `batchBlock`. Not in stream topologies.
  */
sealed trait Batch

/** Requires an ETL topology that owns the PStates being written — a stream or
  * microbatch topology, never a query.
  */
sealed trait Etl

/** What a query topology provides: batch, but no writes. */
sealed trait QueryContext extends Batch

/** What a stream topology provides: writes, but no batch block.
  *
  * The asymmetry with [[QueryContext]] is Rama's, not ours.
  */
sealed trait StreamContext extends Etl

/** What a microbatch topology provides.
  *
  * Extends only [[Etl]] deliberately, though a microbatch topology *can* aggregate:
  * batch is available inside `batchBlock`, not at the top level.
  */
sealed trait MicrobatchContext extends Etl
