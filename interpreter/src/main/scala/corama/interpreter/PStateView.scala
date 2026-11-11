package corama.interpreter

import cats.effect.SyncIO
import cats.syntax.all.*
import corama.*
import corama.interpreter.*
import corama.server.Keyspace

import scala.annotation.unused
import cats.effect.IO

trait PStateView {
	def get(key: PStateKey): SyncIO[Option[Bytes]]
	def select(key: PStateKey, options: SelectOptions.Raw): SyncIO[Vector[Bytes]]
}

object PStateView {
	def inMemory: IO[PStateView] = ???
}

// an encoded, prefixed key composed of (partition, pstate, key)
// TODO: remove double-wrapping
case class PStateKey(raw: Bytes.Value)

object PStateKey {
	// TODO: need to be clear about encoding of prefix, to guarantee no collisions
	// (e.g. `key1|2x` vs `key12|x`)
	// encode as: (pathlen,path,val) so we can tell between (4|key1|2x) and (5|key12|x)
	def forSetElem(partition: PartitionId, pstate: PStateId, path: CompiledPath, env: Env, value: VarId): PStateKey = {
		@unused
		val base = apply(partition, pstate, path, env)
		@unused
		val keypace = Keyspace.PState
		???
	}

	def apply(partition: PartitionId, pstate: PStateId, path: CompiledPath, env: Env): PStateKey = {
		@unused
		val keypace = Keyspace.PState
		???
	}
}

// an immutable snapshot of updates on an underlying base
final class ViewSnapshot(
	base: PStateView,
	updates: Map[PStateKey, Option[Bytes]]
) extends PStateView {
	override def get(key: PStateKey) = {
		updates.get(key).fold(base.get(key))(SyncIO.pure)
	}

	override def select(key: PStateKey, options: SelectOptions.Raw) = ???
}

