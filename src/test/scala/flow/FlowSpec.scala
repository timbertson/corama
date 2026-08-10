package flow

import scala.concurrent.duration._
import cats.syntax.all._
import cats.effect.IO
import weaver.SimpleIOSuite

object FlowSpec extends SimpleIOSuite {

	/** Run `flow` as a query and collect *every* value it emits.
	  *
	  * `aggList` is what makes fan-out observable: a query returns exactly one value,
	  * so a flow that emits 3 has to be collapsed before it can be asserted on. Each
	  * call gets its own `Topologies`, since ids are per-topology and durable names
	  * are checked for duplicates per module.
	  */
	private def emitted[A](input: Int)(flow: Var[Int] => Flow[A]): IO[List[A]] = {
		val ts = Topologies()
		val q = ts.query[Int, List[A]]("q")(t => flow(t.input).aggList)
		new Interpreter(ts).invoke(q, input)
	}

	/** Poll until the predicate holds. Stream topologies are driven by a depot append
	  * consumed on another fiber, so there is no completion signal to await.
	  */
	private def eventually[A](io: IO[A])(p: A => Boolean): IO[A] = {
		def loop: IO[A] = io.flatMap(a => if (p(a)) IO.pure(a) else IO.sleep(10.millis) *> loop)
		loop.timeout(5.seconds)
	}

	/** Every Rama var name a node list mentions, in traversal order. */
	private def namesOf(nodes: List[Node]): List[String] = nodes.flatMap {
		case Node.Each(_, ins, out)        => ins :+ out
		case Node.Explode(_, in, out)      => List(in, out)
		case Node.Filter(_, in, flag)      => List(in, flag)
		case Node.Partition(in)            => List(in)
		case Node.Select(_, key, out)      => List(key, out)
		case Node.Update(_, key, _)        => List(key)
		case Node.Put(_, key, value)       => List(key, value)
		case Node.UpdateWith(_, k, arg, _) => List(k, arg)
		case Node.PartitionAppend(_, arg)  => List(arg)
		case Node.Scope(inner, from, out)  => Nil // handled below
	} .map(RamaBackend.compileVar) ++ nodes.collect {
		case Node.Scope(inner, from, out) =>
			namesOf(inner) ++ List(RamaBackend.compileVar(from), RamaBackend.compileVar(out))
	}.flatten

	// -------------------------------------------------------------------------
	// fan-out semantics
	// -------------------------------------------------------------------------

	test("map is 1-in-1-out") {
		emitted(21)(_.map(_ * 2)).map(vs => expect(vs == List(42)))
	}

	test("explode fans out, and downstream runs once per value") {
		// The core claim. `explode` emits 3 values; the `map` after it must run 3
		// times, not once on a collection. This is what the eager builder's
		// `map`-returns-`this` could not express.
		emitted(1) { input =>
			for {
				n <- input.explode(i => List(i, i + 1, i + 2))
				s <- n.map(_ * 10)
			} yield s
		}.map(vs => expect(vs == List(10, 20, 30)))
	}

	test("explode of empty emits nothing, and downstream never runs") {
		// The counter is a plain `var` mutated from a step body: steps run on whatever
		// fiber the partition is on, but there is only one branch's worth of work here
		// and the read happens after the query returns.
		var ran = 0
		emitted(1) { input =>
			for {
				n <- input.explode(_ => List.empty[Int])
				s <- n.map { v => ran += 1; v }
			} yield s
		}.map(vs => expect.all(vs == Nil, ran == 0))
	}

	test("nested explode multiplies") {
		emitted(1) { input =>
			for {
				a <- input.explode(i => List(i, i * 2))
				b <- a.explode(x => List(x, x + 100))
			} yield b
		}.map(vs => expect(vs == List(1, 101, 2, 102)))
	}

	test("filter prunes branches independently") {
		emitted(6) { input =>
			for {
				n <- input.explode(i => List.range(0, i))
				k <- n.filter(_ % 2 == 0)
			} yield k
		}.map(vs => expect(vs == List(0, 2, 4)))
	}

	test("aggList collapses fan-out back to one branch") {
		// The inverse of explode, and where the type says what Rama's `agg`
		// leaves implicit: Flow[Int] becomes Flow[List[Int]]. Nested inside the
		// harness's own aggList, so the result is a list of one list.
		emitted(4) { input =>
			val inner: Flow[Int] = for {
				n <- input.explode(i => List.range(0, i))
				d <- n.map(_ * 2)
			} yield d
			inner.aggList
		}.map(vs => expect(vs == List(List(0, 2, 4, 6))))
	}

	test("aggList output can be used as a single value again") {
		emitted(10) { input =>
			for {
				all   <- input.explode(i => List(i, i + 1)).aggList
				total <- all.map(_.sum)
			} yield total
		}.map(vs => expect(vs == List(21)))
	}

	test("a partition hop per fanned-out value keeps every branch alive") {
		// In Rama each hop is a separate event, and so a separate transaction. The
		// interpreter really does move the branch to another `Partition`, so this
		// pins that fan-out survives regrouping — the types still enforce nothing.
		emitted(0) { input =>
			for {
				n <- input.explode(i => List(i, i + 1, i + 2))
				_ <- Flow.hashPartition(n)
				d <- n.map(_ * 10)
			} yield d
		}.map(vs => expect(vs.sorted == List(0, 10, 20)))
	}

	test("lowering builds each continuation exactly once") {
		// Rama builds the block once and executes it per value. If `flatMap` built its
		// body per runtime value instead, this counter would read 3 and var names
		// would diverge between the two interpreters.
		var built = 0
		emitted(1) { input =>
			for {
				n <- input.explode(i => List(i, i + 1, i + 2))
				s <- { built += 1; n.map(_ + 1) }
			} yield s
		}.map(vs => expect.all(vs == List(2, 3, 4), built == 1))
	}

	// -------------------------------------------------------------------------
	// PState, via stream topologies
	// -------------------------------------------------------------------------

	test("pstate read-your-writes within one branch") {
		val ts = Topologies()
		val counts = ts.pstate[String, Int]("counts")
		val appends = ts.depot[Int]("appends")
		val seen = ts.pstate[String, Int]("seen")
		ts.stream(appends) { t =>
			for {
				key <- t.input.map(i => s"k${i % 2}")
				_   <- Flow.hashPartition(key)
				_   <- counts.update(key)(prev => Some(prev.getOrElse(0) + 1))
				now <- counts.get(key)
				// Republish the value read back, so the assertion sees what the branch saw
				// rather than only the final state.
				v   <- now.map(_.getOrElse(0))
				_   <- seen.put(key, v)
				done <- Flow.unit
			} yield done
		}

		val interp = new Interpreter(ts)
		interp.running {
			for {
				_ <- List(0, 1, 2, 3, 4).traverse_(interp.emit(appends, _))
				_ <- eventually(interp.get(counts, "k0"))(_ == Some(3))
				_ <- eventually(interp.get(counts, "k1"))(_ == Some(2))
				k0 <- interp.get(seen, "k0")
				k1 <- interp.get(seen, "k1")
			} yield expect.all(k0 == Some(3), k1 == Some(2))
		}
	}

	test("writes from every fanned-out branch land in the store") {
		// Note the shape: to store something *derived from* the fanned-out value we
		// must `map` it into its own var and `put` that, because `update`'s function
		// only sees the current stored value. Reaching for `n` directly inside the
		// update body would capture the `Var`, not the runtime value.
		//
		// Also note the `hashPartition`: a PState is partitioned, so the write has to
		// happen on the partition owning the key or no reader will find it.
		val ts = Topologies()
		val seen = ts.pstate[Int, String]("seen")
		val nums = ts.depot[Int]("nums")
		ts.stream(nums) { t =>
			for {
				n     <- t.input.explode(i => List.range(0, i))
				_     <- Flow.hashPartition(n)
				label <- n.map(v => s"v$v")
				_     <- seen.put(n, label)
				done  <- Flow.unit
			} yield done
		}

		val interp = new Interpreter(ts)
		interp.running {
			for {
				_ <- interp.emit(nums, 3)
				_ <- eventually(interp.getKeys(seen))(_ == Set(0, 1, 2))
				vs <- List(0, 1, 2).traverse(interp.get(seen, _))
			} yield expect(vs == List(Some("v0"), Some("v1"), Some("v2")))
		}
	}

	test("a Var accidentally captured in a step body is a staging bug") {
		// Guarding the hazard the previous check documents. `Var` interpolates into a
		// String silently rather than failing to compile, so what lands in the store is
		// the *name* rather than the value. It can assert on the exact string because
		// ids are positions within one topology: the depot's input var takes 0, the
		// exploded var 1.
		val ts = Topologies()
		val seen = ts.pstate[Int, String]("seen")
		val nums = ts.depot[Int]("nums")
		ts.stream(nums) { t =>
			for {
				n    <- t.input.explode(_ => List(1))
				_    <- Flow.hashPartition(n)
				_    <- seen.update(n)(_ => Some(s"v$n")) // wrong on purpose
				done <- Flow.unit
			} yield done
		}

		val interp = new Interpreter(ts)
		interp.running {
			for {
				_ <- interp.emit(nums, 1)
				v <- eventually(interp.get(seen, 1))(_.isDefined)
			} yield expect(v == Some("vVar(exploded#1)"))
		}
	}

	test("updateWith passes a runtime value into a read-modify-write") {
		// The fix for the hazard above, and the reason it exists: the extra var is
		// passed explicitly rather than closed over, mirroring `Path.term(fn, "*arg")`.
		val ts = Topologies()
		val seen = ts.pstate[Int, String]("seen")
		val nums = ts.depot[Int]("nums")
		ts.stream(nums) { t =>
			for {
				n    <- t.input.explode(_ => List(1))
				_    <- Flow.hashPartition(n)
				_    <- seen.updateWith(n, n) { (_, v) => Some(s"v$v") }
				done <- Flow.unit
			} yield done
		}

		val interp = new Interpreter(ts)
		interp.running {
			for {
				_ <- interp.emit(nums, 1)
				v <- eventually(interp.get(seen, 1))(_.isDefined)
			} yield expect(v == Some("v1"))
		}
	}

	test("partitionAppend feeds one topology from another") {
		val ts = Topologies()
		val source = ts.depot[Int]("source")
		val derived = ts.depot[Int]("derived")
		val doubled = ts.pstate[Int, Int]("doubled")

		ts.stream(source) { t =>
			for {
				n    <- t.input.map(_ * 2)
				_    <- derived.partitionAppend(n)
				done <- Flow.unit
			} yield done
		}
		ts.stream(derived) { t =>
			for {
				_    <- Flow.hashPartition(t.input)
				n    <- t.input.map(identity)
				_    <- doubled.put(n, n)
				done <- Flow.unit
			} yield done
		}

		val interp = new Interpreter(ts)
		interp.running {
			for {
				_ <- interp.emit(source, 21)
				v <- eventually(interp.get(doubled, 42))(_.isDefined)
			} yield expect(v == Some(42))
		}
	}

	// -------------------------------------------------------------------------
	// topology kinds
	// -------------------------------------------------------------------------

	test("a query returns one value; a stream is driven by a depot") {
		// The kinds Rama distinguishes: a query hands a value back to a caller, a
		// stream has nobody to hand one to. `invoke` therefore returns R, not a trace,
		// and `stream` takes the depot that drives it rather than a name.
		val ts = Topologies()
		val appends = ts.depot[Int]("appends")
		val counts = ts.pstate[String, Int]("counts")
		val q = ts.query[Int, Int]("double")(t => t.input.map(_ * 2))
		val s = ts.stream(appends) { t =>
			for {
				k    <- t.input.map(_ => "total")
				_    <- Flow.hashPartition(k)
				_    <- counts.update(k)(prev => Some(prev.getOrElse(0) + 1))
				done <- Flow.unit
			} yield done
		}

		val interp = new Interpreter(ts)
		interp.running {
			for {
				d <- interp.invoke(q, 21)
				_ <- List(1, 2, 3).traverse_(interp.emit(appends, _))
				c <- eventually(interp.get(counts, "total"))(_ == Some(3))
			} yield expect.all(
				d == 42,
				c == Some(3),
				q.builder.source.namespace == IdentityNamespace.Query,
				s.builder.source.namespace == IdentityNamespace.Depot)
		}
	}

	test("invoking a query that fans out is an error, not a silent first value") {
		// A query returns exactly one value. A flow that forgot its `aggList` emits
		// several, and reading `.head` would hide that — so `invoke` fails instead.
		val ts = Topologies()
		val q = ts.query[Int, Int]("fanout")(t => t.input.explode(i => List.range(0, i)))
		IO.defer(new Interpreter(ts).invoke(q, 3)).attempt.map { r =>
			expect(r.left.toOption.exists(_.isInstanceOf[IllegalStateException]))
		}
	}

	test("only a query topology can be invoked") {
		// `invoke` is the client's entry point, and a stream has no result to give it.
		val ts = Topologies()
		val appends = ts.depot[Int]("appends")
		val s = ts.stream(appends)(_ => Flow.unit)
		IO.defer(new Interpreter(ts).invoke(s, 1)).attempt.map(r => expect(r.isLeft))
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
					val _ = ts.depot[Int]("x")
					val _ = ts.query[Int, Int]("x")(t => t.input.map(identity))
					true
				} catch { case _: RuntimeException => false }

			expect.all(
				dup { ts => ts.pstate[Int, Int]("p"); ts.pstate[Int, Int]("p"); () },
				dup { ts => ts.depot[Int]("d"); ts.depot[Int]("d"); () },
				dup { ts =>
					ts.query[Int, Int]("q")(t => t.input.map(identity))
					ts.query[Int, Int]("q")(t => t.input.map(identity))
					()
				},
				sameNameEachKind)
		}
	}

	// -------------------------------------------------------------------------
	// naming and the Rama backend
	// -------------------------------------------------------------------------

	test("var identity is the id, not the hint") {
		// Two vars minted with the same hint are distinct, so nothing has to check
		// whether a hint is already taken.
		IO {
			val ts = Topologies()
			val q = ts.query[Int, Int]("q")(t => t.input.map(identity))
			val a = q.builder.define[Int]("same")
			val b = q.builder.define[Int]("same")
			expect.all(a != b, a == a, RamaBackend.compileVar(a) != RamaBackend.compileVar(b))
		}
	}

	test("ids and Rama names are deterministic per topology") {
		// The point of scoping the counter to a topology: the same program built against
		// a fresh topology gives byte-identical Rama names, with no re-numbering pass.
		// Asserted via the names rather than `nodesA == nodesB`, because a `Node` holds
		// the step's closure and two freshly built closures are never equal. The names
		// carry the ids, so agreeing on them is agreeing on the numbering.
		def build: List[String] = {
			val ts = Topologies()
			val q = ts.query[Int, Int]("q") { t =>
				for {
					n <- t.input.explode(i => List.range(0, i))
					k <- n.filter(_ % 2 == 0)
					s <- k.map(_ * 2)
				} yield s
			}
			namesOf(q.compiled._1).distinct
		}
		IO {
			expect.all(
				build == build,
				build == List("*q_0", "*exploded_1", "*exploded_keep_2", "*each_3"))
		}
	}

	test("compiling twice against one topology advances the counter") {
		// The flip side: a builder is a mutable name source, so a tree is compiled once
		// and the node list shared between backends. Two compilations are
		// α-equivalent, not equal.
		IO {
			val ts = Topologies()
			val q = ts.query[Int, Int]("q")(t => t.input.map(_ * 2))
			val a = Node.compile(q.builder, q.flow)._1
			val b = Node.compile(q.builder, q.flow)._1
			expect.all(a != b, namesOf(a) != namesOf(b))
		}
	}

	test("two filters on one var get distinct keepTrue flags") {
		// The flag is minted during compilation rather than derived from the input name,
		// so it can neither collide with a sibling flag nor shadow a real var.
		val ts = Topologies()
		val q = ts.query[Int, List[Int]]("q") { t =>
			(for {
				n <- t.input.explode(i => List.range(0, i))
				a <- n.filter(_ % 2 == 0)
				b <- a.filter(_ > 2)
			} yield b).aggList
		}
		val flags = q.compiled._1.collect {
			case Node.Scope(inner, _, _) => inner.collect { case f: Node.Filter => RamaBackend.compileVar(f.flag) }
		}.flatten

		new Interpreter(ts).invoke(q, 10).map { vs =>
			expect.all(
				flags == List("*exploded_keep_2", "*exploded_keep_3"),
				vs == List(4, 6, 8))
		}
	}

	test("Var.unit has no Rama representation") {
		// It stands for the absence of a value and is bound only in the interpreter, so
		// compiling a flow that reads it must fail here rather than at deploy time.
		val ts = Topologies()
		val q = ts.query[Int, Int]("q")(_ => Var.unit.map(_ => 1))
		IO(RamaBackend.compile(q)).attempt.map { r =>
			expect(r.left.toOption.exists(_.isInstanceOf[IllegalArgumentException]))
		}
	}

	test("every node kind compiles to a Rama block") {
		// Not asserting on block structure (Rama's Block has no public inspection API)
		// — asserting that compilation of every node kind succeeds, so the reference
		// semantics above describe something real.
		val ts = Topologies()
		val counts = ts.pstate[String, Int]("counts")
		val nums = ts.depot[Int]("nums")
		val echo = ts.depot[Int]("echo")
		val s = ts.stream(nums) { t =>
			for {
				n     <- t.input.explode(i => List(i, i + 1))        // Explode
				_     <- Flow.hashPartition(n)                        // Partition
				key   <- n.map(_.toString)                            // Each (arity 1)
				pair  <- Flow.map2(n, n)((a, b) => s"$a-$b")           // Each (arity 2)
				prev  <- counts.get(key)                              // Select
				_     <- counts.update(key)(p => Some(p.getOrElse(0) + 1))     // Update
				_     <- counts.updateWith(key, n)((p, v) => Some(p.getOrElse(0) + v)) // UpdateWith
				_     <- counts.put(key, n)                           // Put
				_     <- echo.partitionAppend(n)                      // PartitionAppend
				_     <- prev.filter(_ => true)                       // Filter
				all   <- n.explode(i => List(i, i)).aggList           // Scope + agg
				done  <- all.void
			} yield done
		}
		val q = ts.query[Int, List[Int]]("q")(t => t.input.explode(i => List(i, i)).aggList)

		IO {
			val cs = RamaBackend.compile(s)
			val cq = RamaBackend.compile(q)
			expect.all(
				cs.block != null,
				cq.block != null,
				// Rama makes you write two var names by hand: a stream's
				// `.source(depot).out(in)` and a query's `.out("*result")`.
				RamaBackend.compileVar(cs.in) == "*nums_0",
				cs.out == None,
				RamaBackend.compileVar(cq.in) == "*q_0",
				cq.out.map(RamaBackend.compileVar) == Some("*agg_2"))
		}
	}
}
