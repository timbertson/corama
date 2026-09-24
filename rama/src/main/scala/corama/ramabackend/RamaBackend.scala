package corama.rama

import cats.syntax.all._
import com.rpl.rama.{Agg, Block, Path}
import com.rpl.rama
import com.rpl.rama.module.StreamSourceOptions
import com.rpl.rama.ops.{RamaAccumulatorAgg1, RamaFunction1, RamaFunction2, RamaOperation1, OutputCollector}
import com.rpl.rama.RamaModule.Setup
import com.rpl.rama.RamaModule
import corama.IdentityNamespace.Query
import com.rpl.rama.module.StreamTopology
import com.rpl.rama.ops.RamaAccumulatorAgg
import com.rpl.rama.ops.RamaFunction0
import scala.jdk.CollectionConverters._

import corama._
import corama.internal._
import corama.internal.Node.{PStateOp, DynamicOp}
import com.rpl.rama.ops.RamaCombinerAgg
import corama.Schema.ClassSchema
import corama.Schema.FixedKeySchema
import corama.Schema.MapSchema
import corama.Schema.SetSchema
import corama.Subindex.NonIndexed
import corama.Subindex.Indexed
import com.rpl.rama.SubindexOptions

class RamaBackend(setup: RamaModule.Setup, ramaTopologies: RamaModule.Topologies) {
	import RamaBackend._
	def install(topologies: corama.Topologies): Unit = {
		// TODO: would you ever need multiple stream topologies?
		val streamTopology = ramaTopologies.stream("main")

		topologies.ids.foreach { id =>
			id.namespace match {
				// TODO schema + partitioning config
				case corama.IdentityNamespace.Depot => setup.declareDepot(compileIdentity(id), rama.Depot.random())
				// case corama.IdentityNamespace.PState => streamTopology.pstate(compileIdentity(id), classOf[Object])
				case _ => ()
			}
		}

		topologies.pstates.foreach { pstate =>
			val id = compileIdentity(pstate.id)
			compileSchema(pstate.schema) match {
				case Left(schema) => streamTopology.pstate(id, schema)
				case Right(schema) => streamTopology.pstate(id, schema)
			}
		}

		topologies.topologies.foreach { t =>
			val id = t.builder.source
			val compiled = compileNodes(t.compiled._1)

			t.builder.source.namespace match {
				case IdentityNamespace.Depot =>
					// streams always return their final value, which is aggregated.
					// aggregation is omitted for unit streams, for two reasons:
					//  - it's pointless work to perform this aggregation
					//  - it doesn't currently work (Var.unit is virtual, it can't be compiled to a rama var)

					if (t.compiled._2.vid === Var.unit.vid) {
						streamTopology.source(compileIdentity(id)).out(compileVar(t.input))
							.`macro`(compiled)
					} else {
						streamTopology.source(
							compileIdentity(id), StreamSourceOptions.ackReturnAgg(compileCombiner(t.agg))
						).out(compileVar(t.input))
							.`macro`(compiled)
							.ackReturn(compileVar(t.compiled._2))
					}

				case IdentityNamespace.Query =>
					ramaTopologies.query(compileIdentity(id), compileVar(t.input.vid)).out(compileVar(t.queryOutputVar.vid))
						.`macro`(compiled)
						.originPartition()
						.agg(rama.Agg.combiner(compileCombiner(t.agg), compileVar(t.compiled._2.vid))).out(compileVar(t.queryOutputVar.vid))

				case IdentityNamespace.PState => ()
			}
		}
	}
}

object RamaBackend {
	def apply(setup: RamaModule.Setup, topologies: RamaModule.Topologies) = new RamaBackend(setup, topologies)

	private def fn1[A, B](f: A => B): RamaFunction1[A, B] = new RamaFunction1[A, B] {
		def invoke(a: A): B = f(a)
	}

	private def fn0[A](f: () => A): RamaFunction0[A] = new RamaFunction0[A] {
		def invoke(): A = f()
	}

	// generate a rama varname for the given identity
	private def compileIdentity(id: Identity) = {
		id.namespace match {
			case IdentityNamespace.PState => "$$" + id.name
			case IdentityNamespace.Depot => "*" + id.name
			case IdentityNamespace.Query => id.name
		}
	}

	// generate a rama var name
	private[corama] def compileVar(vid: VarId): String = {
		require(vid != Var.unit.vid, "Var.unit has no Rama representation: it stands for the absence of a value")
		// TODO assert hint doesn't have any special characters
		s"*${vid.hint}_${vid.id}"
	}

	private[corama] def compilePathComponent(part: CompiledPath.Component): Any = {
		part match {
			case CompiledPath.Literal(p) => {
				if (p.isInstanceOf[String] && p.asInstanceOf[String].startsWith("*")) {
					throw new IllegalArgumentException("Literal value in path component begins with an asterix: ${p}")
				}
				p
			}
			case CompiledPath.Variable(v) => compileVar(v)
		}
	}

	// generate a rama path
	private[corama] def compilePath(path: CompiledPath): rama.Path.Impl = {
		var p = Path.key(compilePathComponent(path.path.head))
		path.path.tail.foreach { part =>
			// TODO: key vs mapKey?
			p.key(compilePathComponent(part))
		}
		p
	}

	def compileVar[A](v: Var[A]): String = compileVar(v.vid)

	// private def eachStep(fn: List[Any] => Any, ins: List[String], out: String): Block.Impl =
	// 	ins match {
	// 		case Nil => {
	// 			Block.each(fn0[Any](() => fn(Nil))).out(out)
	// 		}
	// 		case List(a) => {
	// 			Block.each(fn1[Any, Any](x => fn(List(x))), a).out(out)
	// 		}
	// 		case List(a, b) =>
	// 			val f2 = new RamaFunction2[Any, Any, Any] {
	// 				def invoke(x: Any, y: Any): Any = fn(List(x, y))
	// 			}
	// 			Block.each(f2, a, b).out(out)
	// 		case other =>
	// 			throw new UnsupportedOperationException(
	// 				s"eachStep: arity ${other.length} not wired up yet (inputs: $other)")
	// 	}


	private def compileNode(node: Node): Block.Impl = node.op match {
		case Node.Dynamic(pos, ins, fn, op) => {
			// TODO: error reporting using the `pos`
			val inVars = ins.map(compileVar)
			def compileOp(op: DynamicOp): Block.Impl = op match {
				case DynamicOp.Map(out) => {
					val each = inVars match {
						case Nil => Block.each(() => fn(Nil))
						case List(a) => Block.each(a => fn(List(a)), a)
						case List(a, b) => Block.each((a,b) => fn(List(a,b)), a, b)
						case other => throw new NotImplementedError(s"map[${other.length}]")
					}
					each.out(compileVar(out))
				}

			/** Fan-out as a Rama operation: emit once per element of the computed sequence.
				*
				* Note this uses `RamaOperation` + `OutputCollector` rather than `Ops.EXPLODE`
				* over a returned collection. This saves converting to a java list.
				*/
				case DynamicOp.Explode(out) => {
					val block = inVars match {
						case List(a) => Block.each(new RamaOperation1[Any] {
							override def invoke(a: Any, collector: OutputCollector) = {
								fn(List(a)).asInstanceOf[Seq[Any]].foreach(x => collector.emit(x))
							}
						}, a)
						case other => throw new NotImplementedError(s"Explode[${other.length}]")
					}
					block.out(compileVar(out))
				}

				case DynamicOp.Filter(condVar) =>
					// compile to a Map, then apply keepTrue
					compileOp(op = DynamicOp.Map(condVar)).`macro`(Block.keepTrue(compileVar(condVar)))
			}
			compileOp(op)
		}

		case Node.If(cond, ifBlock, elseBlock, _) => {
			Block.ifTrue(
				compileVar(cond),
				Block.create().`macro`(compileNodes(ifBlock)),
				Block.create().`macro`(compileNodes(elseBlock)),
			)
		}

		case Node.Partition(in) => Block.hashPartition(compileVar(in))

		case Node.Assign(in, out) =>
			println(s"Assigning ${in} -> ${out}")
			if (in === Var.unit.vid) {
				Block.each(() => ()).out(compileVar(out))
			} else {
				Block.each(identity, compileVar(in)).out(compileVar(out))
			}

		case Node.Combine(block, agg, collectFrom, out) =>
			Block.create()
				.`macro`(compileNodes(block))
				.`macro`(Block.agg(Agg.combiner(compileCombiner(agg), compileVar(collectFrom))).out(compileVar(out)))

		case Node.PStateUpdate(_pos, ps, path, ins, fn) =>
			val ramaPath = compilePath(path)
			// Rama models read-modify-write as a `term` on the path: the function
			// receives the current value (null when absent) and returns the new one.
			val mutation = ins match {
				case Nil => ramaPath.term(current => fn(Option(current), Nil).orNull)
				case List(a) => ramaPath.term((current, a) => fn(Option(current), List(a)).orNull, compileVar(a))
				case List(a, b) => ramaPath.term((current, a, b) => fn(Option(current), List(a, b)).orNull, compileVar(a), compileVar(b))
				case other => throw new NotImplementedError(s"PStateUpdate[${other.length}]")
			}
			Block.localTransform(compileIdentity(ps), mutation)

		case Node.PStateNode(psId, path, op) => {
			println(s"compiling rama path ${path} for op ${op} on ${psId}")
			val ramaPath = compilePath(path)
			val ps = compileIdentity(psId)
			// Rama models read-modify-write as a `term` on the path: the function
			// receives the current value (null when absent) and returns the new one.
			op match {
				// TODO: seems like `nullToSet` would be required when navigating into nonexistent sets.
				// This doesn't work for subindexes sets (it overwrites the value to be a regular HashSet),
				// TBD whether it's actually needed for sets.
				case PStateOp.Add(v: VarId) =>
					Block.localTransform(ps, ramaPath /*.nullToSet() */ .voidSetElem().termVal(compileVar(v)))

				case PStateOp.Remove(v: VarId) =>
					Block.localTransform(ps, ramaPath /*.nullToSet() */ .setElem(compileVar(v)).termVoid())

				case PStateOp.Select(intermediate: VarId, out: VarId, opts) =>
					val setPath = ramaPath // .nullToSet()
					val fullPath = opts match {
						case SelectOptions(Some(after), Some(limit), _) => setPath.sortedSetRangeFrom(compileVar(after), limit)
						case SelectOptions(None, Some(limit), _) => setPath.sortedSetRangeFromStart(limit)
						case SelectOptions(Some(after), None, _) => setPath.sortedSetRangeFrom(compileVar(after))
						case SelectOptions(None, None, _) => setPath
					}
					Block.localSelect(ps, fullPath)
						.out(compileVar(intermediate))
						.each((jlist: java.util.Collection[Any]) => jlist.asScala.toList, compileVar(intermediate))
						.out(compileVar(out))

				case PStateOp.Get(intermediate: VarId, out: VarId) => {
					Block.localSelect(ps, ramaPath)
						.out(compileVar(intermediate))
						.each(Option.apply, compileVar(intermediate))
						.out(compileVar(out))
				}

				case PStateOp.Put(v: VarId) => Block.localTransform(ps, ramaPath.termVal(compileVar(v)))
				// case PStateOp.Delete(v: VarId) => Block.localTransform(ps, ramaPath).termVoid()
			}
		}

		case Node.LocalAppend(dep, value) =>
			Block.depotPartitionAppend(compileIdentity(dep), compileVar(value))
	}

	private def compileNodes(nodes: List[Node]): Block.Impl =
		nodes.foldLeft(Block.create())((acc, n) => acc.`macro`(compileNode(n)))

	private def compileCombiner[O](agg: corama.Agg[O]): RamaCombinerAgg[O] = {
		val zero = agg.zero
		val combineFn = agg.combine
		new RamaCombinerAgg[O] {
			override def zeroVal(): O = zero
			override def combine(current: O, value: O): O = combineFn(current, value)
		}
	}
	
	private def compileSchema(schema: Schema): Either[Class[?], rama.PState.Schema] = {
		println(s"Compiling schema ${schema}")
		def applyIndexing(source: IndexableSchema, result: rama.PState.Schema): rama.PState.Schema = {
			println(s"Applying indexing: ${source.index} to schema: $result")
			source.index match {
				case NonIndexed => result
				case Indexed(sizeTracking) => {
					var opts = SubindexOptions.create()
					if (sizeTracking == false) {
						opts = opts.withoutSizeTracking()
					}
					println(s"Subindexed: $opts")
					result.subindexed(opts)
				}
			}
		}

		schema match {
			case ClassSchema(tag) => Left(tag.closestClass)

			case FixedKeySchema(shape) => {
				val pairs = shape.toList.flatMap {
					case (key, schema) => List(key, compileSchema(schema).merge)
				}
				Right(rama.PState.fixedKeysSchema(pairs*))
			}

			case mapSchema @ MapSchema(k, v, index) => {
				val keyClass = k.tag.closestClass
				Right(applyIndexing(mapSchema, compileSchema(v) match {
					case Left(x) => rama.PState.mapSchema(keyClass, x)
					case Right(x) => rama.PState.mapSchema(keyClass, x)
				}))
			}

			case setSchema @ SetSchema(v, index, setTag, ord) => {
				Right(applyIndexing(setSchema, rama.PState.setSchema(v.tag.closestClass)))
			}
		}
	}
}
