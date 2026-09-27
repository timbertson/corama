package corama.interpreter

import corama.*
import org.scalacheck.Gen
import corama.Flow

object Evaluation {
	type LS = List[String]
	type F = Flow[List[String], StreamContext[List[String]]]

	// Slot to hold vars whose contents isn't known until topology generation time
	// when VarIds are assigned
	class PendingVar {
		var state: Var[LS] = null
		def get = Option(state).getOrElse { throw new RuntimeException("PendingVar not defined") }
		def set(v: Var[LS]) = {
			state = v
		}
	}
	
	case class State(vars: List[PendingVar], flow: F) {
		private def genVar: Gen[Option[PendingVar]] = {
			if (vars.isEmpty) {
				Gen.const(None)
			} else {
				Gen.option(Gen.oneOf(vars))
			}
		}

		// Operate on a random defined var
		def extend(fn: Var[LS] => F): Gen[State] = {
			genVar.map { source =>
				val newVar = new PendingVar
				val newFlow = flow.flatMap { flowVar =>
					newVar.set(flowVar)
					val input = source.getOrElse(newVar).get
					fn(input)
				}
				State(newVar :: vars, newFlow)
			}
		}

		def extendExec(fn: Var[LS] => Flow[Unit, StreamContext[List[String]]]): Gen[State] = {
			genVar.map { source =>
				val newFlow = flow.flatMap { flowVar =>
					val input = source.map(_.get).getOrElse(flowVar)
					fn(input).flatMap(_ => Flow.of(input)) // reexpose input to get back to Flow[String]
				}
				copy(flow = newFlow)
			}
		}
	}

	def map(s: State): Gen[State] = {
		val mapFn = Gen.oneOf[LS => LS](
			Gen.const((l: LS) => l.reverse),
			Gen.stringOfN(3, Gen.alphaChar).map { prefix => (l: LS) => prefix :: l },
			Gen.const((l: LS) => l match {
				case x::xs => xs ++ List(x)
				case Nil => Nil
			}),
		)
		
		mapFn.flatMap { fn =>
			s.extend(_.map(fn))
		}
	}

	// def intFlow: Gen[FlowOf[Int]] = ???

	// def stringFlow: Gen[FlowOf[String]] = ???
	
	// def intToString(f: FlowOf[Int]): Gen[FlowOf[String]] = ???
	// def stringToInt(f: FlowOf[String]): Gen[FlowOf[Int]] = ???
	
	def filter(s: State): Gen[State] = {
		val fn = Gen.oneOf[LS => Boolean](
			l => l.size % 2 == 0,
			l => l.mkString.size % 2 == 0,
		)
		
		fn.flatMap { fn =>
			s.extend(_.filter(fn))
		}
	}

	def explode(s: State): Gen[State] = {
		s.extend(_.explode.flatMap(_.map(List(_))))
	}

	def partition(s: State): Gen[State] = {
		s.extendExec(Flow.hashPartition)
	}

	private val aggOp: Gen[Agg[List[String]]] = {
		val zero = List.empty[String]
		// TODO are there multiple valid aggs?
		Gen.const(Agg.apply(zero, (a,b) => (a ++ b).sorted))
	}

	// def agg(s: State): Gen[F] = {
	// 	aggOp.flatMap()
	// 	s.flow.agg()
	// 	agg.map(f.agg(_))
	// }
	
	val MAX_FLOW_STEPS = 10
	def genFlow(input: Var[LS]): Gen[F] = {
		def extend(s: State, n: Int): Gen[State] = {
			if (n <= 0) {
				Gen.const(s)
			} else {
				Gen.oneOf(
					map(s),
					filter(s),
					explode(s),
					partition(s),
				).flatMap(extend(_, n - 1))
			}
		}
		Gen.choose(0, MAX_FLOW_STEPS).flatMap { n =>
			val root = State(Nil, Flow.of(input))
			extend(root, n).map(_.flow)
		}
	}
	
	case class TopologyCtx(topologies: Topologies, depot: Depot[LS], topology: StreamTopology[LS, LS]) {
	}

	def genTopology: Gen[TopologyCtx] = {
		aggOp.flatMap { agg =>
			// Hack: varid-0 is always the input, due to evaluation order
			val inputVar = new Var[LS](VarId(0, "input"), implicitly, implicitly)

			genFlow(inputVar).flatMap { flow =>
				val t = Topologies()
				val depot = t.depot[LS]("inputs")
				val top = t.stream(depot, agg) { input =>
					assert(input == inputVar)
					flow
				}
				TopologyCtx(t, depot, top)
			}
		}
	}
}
