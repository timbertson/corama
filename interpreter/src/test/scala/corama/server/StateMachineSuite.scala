package corama.server

import cats.effect.IO
import cats.syntax.all.*
import corama.{Bytes, Codec, DepotId, PStateId}
import corama.interpreter.{DepotAppend, DepotAppends, PStateUpdate, PStateUpdates, ReplicationUpdate, TxId}
import corama.interpreter.db.RocksDb
import weaver.SimpleIOSuite
import java.nio.file.Files

object StateMachineSuite extends SimpleIOSuite {
	private val sample = ReplicationUpdate(
		TxId(42L),
		List(DepotAppends(DepotId("depot-1"), Vector(DepotAppend(Array[Byte](1, 2, 3)), DepotAppend(Array.empty)))),
		List(PStateUpdates(PStateId("pstate-1"), Vector(
			PStateUpdate(Array[Byte](4, 5), Some(Array[Byte](6))),
			PStateUpdate(Array[Byte](7), None),
		))),
	)

	test("ReplicationUpdate codec round-trips successfully") {
		IO.delay {
			val codec = ReplogCodecs.replicationUpdate
			val encoded = codec.encode(sample)
			val reencoded = codec.encode(codec.decode(encoded).get)
			val roundTripped = codec.decode(encoded).get
			expect(Bytes.eq(encoded, reencoded))
				.and(expect.eql(roundTripped.id, TxId(42L)))
				.and(expect.eql(roundTripped.depotAppends.size, 1))
				.and(expect.eql(roundTripped.pstateUpdates.size, 1))
		}
	}

	test("apply writes depot records, pstate diffs and apply_offset in one batch") {
		IO.delay(Files.createTempDirectory("corama-sm-test")).flatMap { dir =>
			RocksDb(dir.toString).use { db =>
				IO.delay {
					db.writeSync(StateMachine.applyWrites(sample, index = 5L))
					val depot = DepotId("depot-1")
					val pstate = PStateId("pstate-1")
					val d0 = db.getSync(Keyspace.depotKey(depot, 5L, 0))
					val d1 = db.getSync(Keyspace.depotKey(depot, 5L, 1))
					val s0 = db.getSync(Keyspace.pstateKey(pstate, Array[Byte](4, 5)))
					val s1 = db.getSync(Keyspace.pstateKey(pstate, Array[Byte](7)))
					val offset = db.getSync(Keyspace.applyOffsetKey)
					expect(Bytes.eq(d0.get, Array[Byte](1, 2, 3)))
						.and(expect(Bytes.eq(d1.get, Array.empty)))
						.and(expect(Bytes.eq(s0.get, Array[Byte](6))))
						.and(expect(s1.isEmpty)) // None = delete
						.and(expect(Bytes.eq(offset.get, Codec.forLong.encode(5L))))
				}
			}
		}
	}
}
