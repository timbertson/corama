package corama.server

import cats.effect.IO
import corama.{Bytes, Codec, DepotId, PStateId}
import corama.interpreter.{DepotAppend, DepotAppends, PStateUpdate, ReplicationUpdate}
import corama.interpreter.db.RocksDb
import weaver.SimpleIOSuite
import java.nio.file.Files
import corama.interpreter.Partitioned
import corama.PartitionId
import corama.interpreter.PStateKey
import corama.CompiledPath
import cats.data.NonEmptyList
import corama.Encoded
import izumi.reflect.Tag
import cats.Show

object StateMachineSuite extends SimpleIOSuite {
	private val partition0 = PartitionId(0)
	private val partition1 = PartitionId(1)
	private val depot = DepotId("depot-1")
	private val pstate = PStateId("pstate-1")
	private val pstateKey1 = PStateKey(
		partition0,
		pstate,
		CompiledPath(
			NonEmptyList.of(CompiledPath.Literal(Encoded("key1").asAny)),
			terminalTag = implicitly[Tag[Bytes]].tag,
			codec = implicitly[Codec[Bytes]].asAny,
		),
		env = Map.empty
	)

	private val pstateKey2 = PStateKey(
		partition1,
		PStateId("pstate-1"),
		CompiledPath(
			NonEmptyList.of(CompiledPath.Literal(Encoded("key2").asAny)),
			terminalTag = implicitly[Tag[Bytes]].tag,
			codec = implicitly[Codec[Bytes]].asAny,
		),
		env = Map.empty
	)

	private val pstateKey3 = PStateKey(
		partition1,
		PStateId("pstate-1"),
		CompiledPath(
			NonEmptyList.of(CompiledPath.Literal(Encoded("key3").asAny)),
			terminalTag = implicitly[Tag[Bytes]].tag,
			codec = implicitly[Codec[Bytes]].asAny,
		),
		env = Map.empty
	)

	private val sample = ReplicationUpdate(
		List(DepotAppends(
			depot,
			Array(
				Partitioned(partition0, DepotAppend(Array[Byte](1, 2, 3))),
				Partitioned(partition1, DepotAppend(Array.empty))),
			),
		),
		Array(
			PStateUpdate(pstateKey1, value = Some(Array[Byte](4, 5))),
			PStateUpdate(pstateKey2, value = None),
			PStateUpdate(pstateKey3, value = Some(Array[Byte](6))),
		)
	)

	pureTest("ReplicationUpdate codec round-trips successfully") {
		val codec = ServerCodecs.replicationUpdate
		val encoded = codec.encode(sample)
		val reencoded = codec.encode(codec.decode(encoded).get)
		val roundTripped = codec.decode(encoded).get
		expect(Bytes.eq(encoded, reencoded))
			.and(expect.eql(roundTripped.depotAppends.size, 1))
			.and(expect.eql(roundTripped.pstateUpdates.size, 3))
	}

	test("apply writes depot records, pstate diffs and apply_offset in one batch") {
		implicit val showBytes: Show[Array[Byte]] = Show.show { b =>
			b.mkString("{",",","}")
		}
		IO.delay(Files.createTempDirectory("corama-sm-test")).flatMap { dir =>
			RocksDb(dir.toString).use { db =>
				for {
					_ <- db.write(StateMachine.applyWrites(sample, index = 5L))
					d0 <- db.get(Keyspace.depotKey(partition0, depot, 5L, 0))
					d1 <- db.get(Keyspace.depotKey(partition1, depot, 5L, 1))
					s0 <- db.get(pstateKey1.raw.raw)
					s1 <- db.get(pstateKey2.raw.raw)
					offset <- db.get(Keyspace.applyOffsetKey)
				} yield {
					expect(Bytes.eq(clue(d0.get), Array[Byte](1, 2, 3)))
						.and(expect(Bytes.eq(clue(d1.get), Array.empty)))
						.and(expect(Bytes.eq(clue(s0.get), Array[Byte](4, 5))))
						.and(expect(clue(s1).isEmpty)) // None = delete
						.and(expect(Bytes.eq(clue(offset.get), Codec.forLong.encode(5L))))
				}
			}.to[IO]
		}
	}
}
