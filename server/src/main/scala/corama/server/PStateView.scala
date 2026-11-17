package corama.server

import cats.effect.SyncIO
import cats.syntax.all.*
import corama.*

import cats.effect.IO
import corama.CompiledPath.Literal
import corama.CompiledPath.Variable
import java.io.ByteArrayOutputStream
import corama.server.Env

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
		val bs = build(Keyspace.PStateSet, partition, pstate, path, env)
		bs.write(value.codec.encode(env(value)))
		PStateKey(Bytes.Value(bs.toByteArray))
	}
	
	def apply(partition: PartitionId, pstate: PStateId, path: CompiledPath, env: Env): PStateKey = {
		val bs = build(Keyspace.PStateKv, partition, pstate, path, env)
		PStateKey(Bytes.Value(bs.toByteArray()))
	}

	private def build(
		keyspace: Byte,
		partition: PartitionId,
		pstate: PStateId,
		path: CompiledPath,
		env: Env
	): ByteArrayOutputStream = {
		val bs = new ByteArrayOutputStream()
		bs.write(keyspace)
		bs.write(Codec[Long].encode(partition.id))
		bs.write(Codec[String].encode(pstate.value))
		path.path.toList.foreach { part =>
			bs.write(part match {
				case Literal(inner) => inner.bytes
				case Variable(vid) => vid.codec.encode(env(vid))
			})
		}
		bs
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

