package corama.interpreter

import corama.*
import org.scalacheck.Gen
import corama.Flow
import cats.data.NonEmptyList
import izumi.reflect.Tag

object Evaluation {

	enum TypeEnum {
		case Int
		case Str
	}

	type LS = List[String]
	type LI = List[Int]
	type Ctx = StreamContext[List[String]]
	type Fl[A] = Flow[List[A], Ctx]
	type FS = Flow[List[String], Ctx]
	type FI = Flow[List[Int], Ctx]

	trait Generics[T] {
		val tag: Tag[T]
		val typeEnum: TypeEnum
		val mapFn: Gen[List[T] => List[T]]
		val filterFn: Gen[List[T] => Boolean]
		def aggOp: Gen[Agg[List[T]]]
		
		def toStringFn: Gen[List[T] => List[String]]
		def toIntFn: Gen[List[T] => List[Int]]
	}
	
	implicit val stringGenerics: Generics[String] = new Generics[String] {
		override val tag: Tag[String] = implicitly
		override val typeEnum: TypeEnum = TypeEnum.Str

		override val mapFn: Gen[LS => LS] = Gen.oneOf(
				Gen.const((l: LS) => l.reverse),
				Gen.stringOfN(3, Gen.alphaChar).map { prefix => (l: LS) => prefix :: l },
				Gen.const((l: LS) => l match {
					case x::xs => xs ++ List(x)
					case Nil => Nil
				}),
			)

		override val filterFn: Gen[List[String] => Boolean] = Gen.oneOf(
			l => l.size % 2 == 0,
			l => l.mkString.size % 2 == 0,
		)

		override val aggOp: Gen[Agg[List[String]]] = {
			// TODO are there multiple valid aggs?
			Gen.const(Agg.apply(List.empty[String], (a,b) => (a ++ b).sorted))
		}
	
		override def toStringFn: Gen[List[String] => List[String]] = Gen.const(identity)

		override def toIntFn: Gen[List[String] => List[Int]] = Gen.oneOf(
			(ls: LS) => List(ls.size),
			(ls: LS) => ls.map(_.size),
		)
	}

	implicit val intGenerics: Generics[Int] = new Generics[Int] {
		override val tag: Tag[Int] = implicitly
		override val typeEnum: TypeEnum = TypeEnum.Int

		override val mapFn: Gen[List[Int] => List[Int]] = Gen.oneOf(
				Gen.const((l: LI) => l.reverse),
				Gen.choose(0, 500).map { prefix => (l: LI) => prefix :: l },
				Gen.choose(-100, 100).map { addition => (l: LI) => l.map(_ + addition) },
			)

		override val filterFn: Gen[LI => Boolean] = Gen.oneOf(
			l => l.size % 2 == 0,
			l => l.sum % 2 == 0,
		)

		override val aggOp: Gen[Agg[List[Int]]] = {
			// TODO are there multiple valid aggs?
			Gen.const(Agg.apply(Nil, (a,b) => (a ++ b).sorted))
		}
		
		override def toStringFn: Gen[List[Int] => List[String]] = Gen.const { ls => ls.map(_.toString) }

		override def toIntFn: Gen[List[Int] => List[Int]] = Gen.const(identity)
	}

	// Slot to hold vars whose contents isn't known until topology generation time
	// when VarIds are assigned
	class PendingVar[A] {
		var state: Var[List[A]] = null
		def get = Option(state).getOrElse { throw new RuntimeException("PendingVar not defined") }
		def set(v: Var[List[A]]) = {
			state = v
		}
	}
	
	case class State[A](varMap: Map[TypeEnum, List[PendingVar[Any]]], flow: Fl[A])(using Generics[A]) {
		private def genVar[B](using g: Generics[B]): Gen[Option[PendingVar[B]]] = {
			val vars = varMap.getOrElse(g.typeEnum, Nil).asInstanceOf[List[PendingVar[B]]]
			if (vars.isEmpty) {
				Gen.const(None)
			} else {
				Gen.option(Gen.oneOf(vars))
			}
		}
		
		private def withVar[B](v: PendingVar[B])(using g: Generics[B]): Map[TypeEnum, List[PendingVar[Any]]] = {
			varMap.updatedWith(g.typeEnum) { existing =>
				Some(v.asInstanceOf[PendingVar[Any]] :: existing.getOrElse(Nil))
			}
		}

		// Operate on a random defined var
		def extend[B](fn: Var[List[A]] => Fl[B])(using Generics[B]): Gen[State[B]] = {
			genVar[A].map { sourceOpt =>
				val newVar = new PendingVar[A]
				val newFlow = flow.flatMap { flowVar =>
					newVar.set(flowVar)
					val input = sourceOpt.getOrElse(newVar).get
					fn(input)
				}
				State(withVar(newVar), newFlow)
			}
		}

		def extendExec(fn: Var[List[A]] => Flow[Unit, StreamContext[List[String]]]): Gen[State[A]] = {
			genVar.map { source =>
				val newFlow = flow.flatMap { flowVar =>
					val input = source.map(_.get).getOrElse(flowVar)
					fn(input).flatMap(_ => Flow.of(input)) // reexpose input to get back to Flow[String]
				}
				copy(flow = newFlow)
			}
		}
	}

	def map[A](s: State[A])(using g: Generics[A]): Gen[State[A]] = {
		given Tag[A] = g.tag
		g.mapFn.flatMap { fn =>
			s.extend(_.map[List[A]](fn))
		}
	}

	def filter[A](s: State[A])(using g: Generics[A]): Gen[State[A]] = {
		g.filterFn.flatMap { fn =>
			s.extend(_.filter(fn))
		}
	}

	def explode[A](s: State[A])(using g: Generics[A]): Gen[State[A]] = {
		given Tag[A] = g.tag
		s.extend(_.explode.flatMap(_.map(List(_))))
	}

	def partition[A](s: State[A]): Gen[State[A]] = {
		s.extendExec(Flow.hashPartition)
	}

	private val aggOp: Gen[Agg[List[String]]] = {
		val zero = List.empty[String]
		// TODO are there multiple valid aggs?
		Gen.const(Agg.apply(zero, (a,b) => (a ++ b).sorted))
	}
	
	// def agg[A](s: State[A])(using g: Generics[A]): Gen[State[A]] = {
	// 	for {
	// 		agg <- g.aggOp
	// 		subFlow <- genSimpleFlow(s.flow)
	// 		result <- s.extend { _input =>
	// 			subFlow.agg(agg)
	// 		}
	// 	} yield result
	// }
	
	val MAX_FLOW_STEPS = 10
	def genFlow(root: Fl[String]) : Gen[Fl[String]] = {
		Gen.choose(0, MAX_FLOW_STEPS).flatMap { n =>
			val rootState = State(Map.empty, root)
			extendString(n, rootState).map(_.flow)
		}
	}

	def genSimpleFlow[A](root: Fl[A])(using Generics[A]): Gen[Fl[A]] = {
		Gen.choose(0, MAX_FLOW_STEPS).flatMap { n =>
			val rootState = State(Map.empty, root)
			extendSimple(n, rootState).map(_.flow)
		}
	}

	private def simpleOp[A](s: State[A])(using Generics[A]): Gen[State[A]] = {
		Gen.frequency(
			3 -> map(s),
			1 -> filter(s),
			1 -> explode(s),
			5 -> partition(s),
		)
	}

	private def extendSimple[A](n: Int, s: State[A])(using Generics[A]): Gen[State[A]] = {
		if (n <= 0) {
			Gen.const(s)
		} else {
			simpleOp(s).flatMap(extendSimple(n-1, _))
		}
	}
	
	private def extendString(n: Int, s: State[String])(using Generics[String])(using Generics[Int]): Gen[State[String]] = {
		if (n <= 0) {
			Gen.const(s)
		} else {
			Gen.frequency(
				10 -> simpleOp(s).flatMap(extendString(n-1, _)),
				2 -> stringToInt(s).flatMap(extendInt(n-1, _)),
			)
		}
	}

	private def extendInt(n: Int, s: State[Int])(using Generics[String])(using Generics[Int]): Gen[State[String]] = {
		if (n <= 0) {
			// need to map back to string as the final result
			intToString(s)
		} else {
			Gen.frequency(
				10 -> simpleOp(s).flatMap(extendInt(n-1, _)),
				2 -> intToString(s).flatMap(extendString(n-1, _)),
			)
		}
	}

	private def stringToInt(s: State[String])(using g: Generics[String])(using Generics[Int]) = {
		g.toIntFn.flatMap { fn =>
			s.extend(_.map(fn))
		}
	}

	private def intToString(s: State[Int])(using g: Generics[Int])(using Generics[String]) = {
		g.toStringFn.flatMap { fn =>
			s.extend(_.map(fn))
		}
	}

	case class TopologyCtx(root: Topologies, depot: Depot[LS], topologies: NonEmptyList[StreamTopology[LS, LS]])

	def genTopology: Gen[TopologyCtx] = {
		aggOp.flatMap { agg =>
			// Hack: varid-0 will always be the input var generated in `t.stream`, due to evaluation order
			val inputVar = new Var[LS](VarId(0, "input"), implicitly, implicitly)

			for {
				numFlows <- Gen.choose(1, 4)
				flows <- Gen.listOfN(numFlows, genFlow(Flow.of(inputVar)))
			} yield {
				val t = Topologies()
				val depot = t.depot[LS]("inputs")
				val topologies = NonEmptyList.fromListUnsafe(flows).map { flow =>
					t.stream(depot, agg) { input =>
						assert(input == inputVar)
						flow
					}
				}
				TopologyCtx(t, depot, topologies)
			}
		}
	}
}
