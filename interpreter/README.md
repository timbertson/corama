# TODO:

 - leadership elections
 - nodes returning from being offline
 - retries due to network errors
 - versioning identity (protect against execution across mismatched nodes)
 - backpressure for incoming depot appends

 - batching should be done for multiple appends, currently it only really helps for fanouts



# Naming and organisation

Execution begins on a partition. E.g. when triggered by a depot append, it's that depot's partition.

Execution installs a `EvalReceiver` slot in the global `EvalReceivers` registry. This setup produces an `Origin` object which is passed along when the computation jumps to other partitions. Any instance can notify the origin partition of results by invoking that instance's `AcceptEval` GRPC service.

## Eval machinery

Only the active leader for a task performs evaluation, for all partitions in that task.

Evaluation interacts with the cluster in two ways:

 - execution is shifted to another partition via the `TaskRouter` instance
 - eval results are returned to the origin via the `EvalRouter` instance

Note that a `LocalEvalSink` also implements `EvalSink`, for when the continuation happens to be running on the local instance.
(TODO: this is not short-circuited when the continuation returns to the origin instance yet).


To track evaluations for a specific topology + source event:

 - a random `EvalId` is created for every source operation (i.e. each root value triggering a topology)
 - an `Origin` object captures the origin instance and `EvalId`. This is passed along whenever the computation changes partition.
 - Topology execution starts with a single `EdgeId` (randomly generated)
 - On fanout, new `EdgeId`s are created and added into the evaluation represented by EvalId.
 - The origin service is notified of Edge creation/completion, and also results

To track evaluations for a single source event across multiple topologies, `EvalResults` keeps a map of topologyId -> LocalEvalSink.


## Replication machinery

Each task has 3 replica instances. Only these instances are candidates to lead the task.

When acting as leader, all writes from evals are forwarded to replicas. When acting as a replica, relpicated writes are applied to the local copy of depots / pstates.

## Cluster / instance structure

**Interpreter**: In-process emulation of a cluster, used in tests

**ClusterMetadata**: Holds information about leadership, instances, etc. Responsible for receiving updates about leadership changes.

**ClusterInstance**: The root execution for an instance in the cluster. Responsible for:
 - running the logic for partitions led by this instance
 - stopping/starting partition execution as leadership changes
 - running the GRPC services (AcceptEval, Replicate) to receive comms from other instances

**Partition**: implementation of a partition. Can only be created on a replica for a given partition. Should only be invoked when the instance is a leader for the partition's task.

## Storage structure

.. TODO
