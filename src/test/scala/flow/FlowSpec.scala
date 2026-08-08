package flow

import cats.effect.IO
import weaver.SimpleIOSuite

/** Executable checks on fan-out semantics.
  *
  * No cluster: [[Interp]] is plain Scala over an in-memory store, so everything the
  * string-var API cannot state — empty explode runs nothing downstream, nested
  * explode multiplies, `aggList` collapses a region — is assertable here in
  * milliseconds. The `RamaBackend` checks are the slow ones; they pay for
  * Clojure namespace loading on first touch of `com.rpl.rama`. Run with `sbt test`.
  *
  * The bodies are synchronous by nature; they sit in `IO` so the two checks that
  * count builder/step invocations keep their mutation inside an effect.
  */
object FlowSpec extends SimpleIOSuite {

	/** A fresh topology builder and its input var, per test.
	  *
	  * Per-test rather than shared: var ids come from the builder's registry, so a
	  * shared one would make every test's ids depend on which tests ran before it.
	  * `Stream` kind, since most of these write PState — which a query may not.
	  */
	private def fixture: (TopologyBuilder[Int], Var[Int]) = {
		val t = new TopologyBuilder[Int](TopologyId(0), TopologyKind.Stream, "input", "input")
		(t, t.input)
	}

	/** PStates are declared on `Topologies`, which rejects duplicate names — so tests
	  * that declare one need their own container.
	  */
	private def pstate[K, V](name: String): PState[K, V] = Topologies().pstate[K, V](name)

	test("map is 1-in-1-out") {
		IO {
			val (topo, input) = fixture
			val flow = for { doubled <- input.map(_ * 2) } yield doubled
			expect(Interp.runOne(topo, 21, flow).values == List(42))
		}
	}

	test("explode fans out, and downstream runs once per value") {
		// The core claim. `explode` emits 3 values; the `map` after it must run 3
		// times, not once on a collection. This is what the eager builder's
		// `map`-returns-`this` could not express.
		IO {
			val (topo, input) = fixture
			val flow = for {
				n <- input.explode(i => List(i, i + 1, i + 2))
				s <- n.map(_ * 10)
			} yield s
			expect(Interp.runOne(topo, 1, flow).values == List(10, 20, 30))
		}
	}

	test("explode of empty emits nothing, and downstream never runs") {
		IO {
			val (topo, input) = fixture
			var ran = 0
			val flow = for {
				n <- input.explode(_ => List.empty[Int])
				s <- n.map { v => ran += 1; v }
			} yield s
			val values = Interp.runOne(topo, 1, flow).values
			expect.all(values == Nil, ran == 0)
		}
	}

	test("nested explode multiplies") {
		IO {
			val (topo, input) = fixture
			val flow = for {
				a <- input.explode(i => List(i, i * 2))
				b <- a.explode(x => List(x, x + 100))
			} yield b
			expect(Interp.runOne(topo, 1, flow).values == List(1, 101, 2, 102))
		}
	}

	test("filter prunes branches independently") {
		IO {
			val (topo, input) = fixture
			val flow = for {
				n <- input.explode(i => List.range(0, i))
				_ <- n.filter(_ % 2 == 0)
			} yield n
			expect(Interp.runOne(topo, 6, flow).values == List(0, 2, 4))
		}
	}

	test("aggList collapses fan-out back to one branch") {
		// The inverse of explode, and where the type says what Rama's `agg`
		// leaves implicit: Flow[Int] becomes Flow[List[Int]].
		IO {
			val (topo, input) = fixture
			val inner: Flow[Int] = for {
				n <- input.explode(i => List.range(0, i))
				d <- n.map(_ * 2)
			} yield d
			val flow = for { all <- inner.aggList } yield all
			expect(Interp.runOne(topo, 4, flow).values == List(List(0, 2, 4, 6)))
		}
	}

	test("aggList output can be exploded again") {
		IO {
			val (topo, input) = fixture
			val inner: Flow[Int] = input.explode(i => List(i, i + 1))
			val flow = for {
				all <- inner.aggList
				total <- all.map(_.sum)
			} yield total
			expect(Interp.runOne(topo, 10, flow).values == List(21))
		}
	}

	test("partition hops are recorded once per branch") {
		// A partition boundary per fanned-out value: in Rama each of these is a
		// separate event, and so a separate transaction. The trace makes the
		// count assertable even though the types don't yet enforce anything.
		IO {
			val (topo, input) = fixture
			val flow = for {
				n <- input.explode(i => List(i, i + 1, i + 2))
				_ <- Flow.hashPartition(n)
			} yield n
			val trace = Interp.runOne(topo, 0, flow)
			expect.all(
				trace.values == List(0, 1, 2),
				trace.partitionKeys == List(0, 1, 2))
		}
	}

	test("pstate read-your-writes within one branch") {
		IO {
			val (topo, input) = fixture
			val counts = pstate[String, Int]("counts")
			val flow = for {
				key <- input.map(i => s"k${i % 2}")
				_ <- counts.update(key)(prev => Some(prev.getOrElse(0) + 1))
				now <- counts.get(key)
			} yield now
			val (traces, _) = Interp.runAll(topo, List(0, 1, 2, 3, 4), flow)
			expect(traces.flatMap(_.values) == List(Some(1), Some(1), Some(2), Some(2), Some(3)))
		}
	}

	test("writes from every fanned-out branch land in the store") {
		// Note the shape: to store something *derived from* the fanned-out value we
		// must `map` it into its own var and `put` that, because `update`'s function
		// only sees the current stored value. Reaching for `n` directly inside the
		// update body would capture the `Var`, not the runtime value.
		IO {
			val (topo, input) = fixture
			val seen = pstate[Int, String]("seen")
			val flow = for {
				n <- input.explode(i => List.range(0, i))
				label <- n.map(v => s"v$v")
				_ <- seen.put(n, label)
			} yield n
			val (_, store) = Interp.runAll(topo, List(3), flow)
			expect(store.snapshot ==
				Map(("seen", 0) -> "v0", ("seen", 1) -> "v1", ("seen", 2) -> "v2"))
		}
	}

	test("a Var accidentally captured in a step body is a staging bug") {
		// Guarding the hazard the previous check documents. `Var` interpolates into a
		// String silently rather than failing to compile, so what lands in the store is
		// the *name* rather than the value.
		IO {
			val (topo, input) = fixture
			val seen = pstate[Int, String]("seen")
			val flow = for {
				n <- input.explode(_ => List(1))
				_ <- seen.update(n)(_ => Some(s"v$n")) // wrong on purpose
			} yield n
			val (_, store) = Interp.runAll(topo, List(1), flow)
			expect(store.snapshot == Map(("seen", 1) -> "vVar(exploded#1)"))
		}
	}

	test("lowering builds each continuation exactly once") {
		// Rama builds the block once and executes it per value. If `flatMap` built
		// its body per runtime value instead, this counter would read 3 and var
		// names would diverge between the two interpreters.
		IO {
			val (topo, input) = fixture
			var built = 0
			val flow = for {
				n <- input.explode(i => List(i, i + 1, i + 2))
				s <- { built += 1; n.map(_ + 1) }
			} yield s
			val trace = Interp.runOne(topo, 1, flow)
			expect.all(trace.values == List(2, 3, 4), built == 1)
		}
	}

	test("the same flow compiles to a Rama block") {
		// Not asserting on block structure (Rama's Block has no public
		// inspection API) — asserting that compilation of every node kind
		// succeeds, so the reference semantics above describe something real.
		IO {
			val (topo, input) = fixture
			val counts = pstate[String, Int]("counts")
			val flow = for {
				n <- input.explode(i => List(i, i + 1))
				_ <- Flow.hashPartition(n)
				key <- n.map(_.toString)
				prev <- counts.get(key)
				_ <- counts.update(key)(p => Some(p.getOrElse(0) + 1))
				_ <- prev.filter(_ => true)
			} yield n
			expect(RamaBackend.compileFlow(topo, flow) != null)
		}
	}

	test("aggList compiles to a Rama block") {
		IO {
			val (topo, input) = fixture
			val inner: Flow[Int] = input.explode(i => List(i, i + 1))
			expect(RamaBackend.compileFlow(topo, inner.aggList) != null)
		}
	}

	test("var identity is the id, not the hint") {
		// Two vars minted with the same hint are distinct, so `Lower` never has to
		// check whether a hint is already taken.
		IO {
			val (topo, _) = fixture
			val a = topo.variable[Int]("same")
			val b = topo.variable[Int]("same")
			expect.all(a != b, a == a, RamaBackend.varName(a) != RamaBackend.varName(b))
		}
	}

	test("ids and Rama names are deterministic per topology") {
		// The point of scoping the counter to a topology: the same program built against
		// a fresh topology gives byte-identical Rama names, with no re-numbering pass.
		// Asserted via the names rather than `nodesA == nodesB`, because a `Node` holds
		// the step's closure and two freshly built closures are never equal. The names
		// carry the ids, so agreeing on them is agreeing on the numbering.
		def build: List[String] = {
			val (topo, input) = fixture
			val flow = for {
				n <- input.explode(i => List.range(0, i))
				_ <- n.filter(_ % 2 == 0)
				s <- n.map(_ * 2)
			} yield s
			RamaBackend.namesOf(Lower(topo, flow)._1).distinct
		}
		IO {
			val namesA = build
			val namesB = build
			expect.all(
				namesA == namesB,
				namesA == List("*input_0", "*exploded_1", "*exploded_keep_2", "*each_3"))
		}
	}

	test("lowering twice against one topology advances the counter") {
		// The flip side: a topology is a mutable name source, so a tree must be lowered
		// once and shared. Two lowerings are α-equivalent, not equal.
		IO {
			val (topo, input) = fixture
			def mk: Flow[Int] = input.map(_ * 2)
			val a = Lower(topo, mk)._1
			val b = Lower(topo, mk)._1
			expect.all(a != b, RamaBackend.namesOf(a) != RamaBackend.namesOf(b))
		}
	}

	test("two filters on one var get distinct keepTrue flags") {
		// The flag is minted by `Lower` rather than derived from the input name, so it
		// can neither collide with a sibling flag nor shadow a real var.
		IO {
			val (topo, input) = fixture
			val flow = for {
				n <- input.explode(i => List.range(0, i))
				_ <- n.filter(_ % 2 == 0)
				_ <- n.filter(_ > 2)
			} yield n
			val nodes = Lower(topo, flow)._1
			val flags = nodes.collect { case f: Node.Filter => RamaBackend.varName(f.flag) }
			expect.all(
				flags == List("*exploded_keep_2", "*exploded_keep_3"),
				RamaBackend.compile(nodes) != null,
				Interp.runOne(topo, 10, flow).values == List(4, 6, 8))
		}
	}

	test("Var.unit has no Rama representation") {
		// It stands for the absence of a value and is bound only in the interpreter, so
		// compiling a flow that reads it must fail here rather than at deploy time.
		IO {
			val (topo, _) = fixture
			val flow = Var.unit.map(_ => 1)
			val failed =
				try { RamaBackend.compileFlow(topo, flow); false }
				catch { case _: IllegalArgumentException => true }
			expect(failed)
		}
	}

	test("a query returns one value; a stream returns nothing") {
		// The kinds Rama distinguishes: `topologies.query(name, "*arg").out("*result")`
		// hands a value back to a caller, `topologies.stream(depot)` has nobody to hand
		// one to. `invoke` therefore returns R rather than a trace.
		IO {
			val ts = Topologies()
			val q = ts.query[Int, Int]("double", arg = "n")(t => t.input.map(_ * 2))
			val s = ts.stream[Int]("appends", arg = "n") { t =>
				val counts = ts.pstate[String, Int]("counts")
				for {
					k <- t.input.map(_ => "total")
					_ <- counts.update(k)(prev => Some(prev.getOrElse(0) + 1))
					done <- Flow.unit
				} yield done
			}
			expect.all(
				Interp.invoke(q, 21) == 42,
				q.kind == TopologyKind.Query,
				s.kind == TopologyKind.Stream,
				Interp.run(s, List(1, 2, 3)).length == 3)
		}
	}

	test("a query topology may not write PState") {
		// Rama rejects this when the topology is declared. Here it's caught at lowering
		// instead — declaring only builds the `Flow`, and a `Flow` is written
		// independently of the kind it lands in — so the failure surfaces on the first
		// `compileTopology`/`run` rather than at the `query(...)` call.
		IO {
			val ts = Topologies()
			val counts = ts.pstate[Int, Int]("counts")
			val bad = ts.query[Int, Unit]("bad")(t => counts.update(t.input)(_ => Some(1)))
			def failsWith(f: => Any): Boolean =
				try { f; false } catch { case _: IllegalArgumentException => true }
			expect.all(
				failsWith(RamaBackend.compileTopology(bad)),
				failsWith(Interp.run(bad, List(1))))
		}
	}

	test("invoking a query that fans out is an error, not a silent first value") {
		// A query returns exactly one value. A flow that forgot its `aggList` emits
		// several, and reading `.head` would hide that — so `invoke` fails instead.
		IO {
			val ts = Topologies()
			val q = ts.query[Int, Int]("fanout")(t => t.input.explode(i => List.range(0, i)))
			val failed =
				try { Interp.invoke(q, 3); false }
				catch { case _: IllegalStateException => true }
			expect(failed)
		}
	}

	test("compiled topologies report the names Rama needs positionally") {
		// A query names its result (`.out("*result")`), a stream has none to name.
		IO {
			val ts = Topologies()
			val q = ts.query[Int, Int]("double", arg = "n")(t => t.input.map(_ * 2))
			val s = ts.stream[Int]("appends", arg = "n")(t => t.input.map(_ * 2).void)
			val cq = RamaBackend.compileTopology(q)
			val cs = RamaBackend.compileTopology(s)
			expect.all(
				cq.in == "*n_0",
				cq.out == Some("*each_1"),
				cq.name == "double",
				cs.in == "*n_0",
				cs.out == None,
				cs.name == "appends")
		}
	}

	test("durable names are checked for duplicates, per namespace") {
		// PState, depot and query names are separate namespaces in Rama, so the same
		// string may name one of each — but not two of the same kind.
		IO {
			def dup(f: Topologies => Unit): Boolean =
				try { f(Topologies()); false } catch { case _: RuntimeException => true }

			val sameNameEachKind =
				try {
					val ts = Topologies()
					val _ = ts.pstate[Int, Int]("x")
					val _ = ts.stream[Int]("x")(_ => Flow.unit)
					val _ = ts.query[Int, Int]("x")(t => t.input.map(identity))
					true
				} catch { case _: RuntimeException => false }

			expect.all(
				dup { ts => ts.pstate[Int, Int]("p"); ts.pstate[Int, Int]("p"); () },
				dup { ts => ts.stream[Int]("d")(_ => Flow.unit); ts.stream[Int]("d")(_ => Flow.unit); () },
				dup { ts =>
					ts.query[Int, Int]("q")(t => t.input.map(identity))
					ts.query[Int, Int]("q")(t => t.input.map(identity))
					()
				},
				sameNameEachKind)
		}
	}
}
