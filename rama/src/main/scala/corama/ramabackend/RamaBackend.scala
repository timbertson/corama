package corama.rama

import cats.syntax.all._
import com.rpl.rama.{Agg, Block, Path}
import com.rpl.rama
import com.rpl.rama.module.StreamSourceOptions
import com.rpl.rama.ops.{RamaFunction1, RamaFunction2, RamaOperation1, OutputCollector}
import com.rpl.rama.RamaModule.Setup
import com.rpl.rama.RamaModule
import corama.IdentityNamespace.Query
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
import corama.rama.CodecRegistry.tagAny
import Tagged.untagAny
import izumi.reflect.Tag
import izumi.reflect.macrortti.LightTypeTag

/*
Note on serialization in the rama backend:

Rama's serialization is based on concrete, erased types - it's not possible to define a serializer for List[A] given a serializer for A. You can only define
a serializer for all lists.

Idiomatic rama usage gets around this in two ways:
 - use untyped objects (arrays / maps containing primitives which are each already supported)
 - use concrete classes for each serializable object (ListOfUsers, etc)

This backend relies on the `Codec` typeclass, allowing unobtrusive and generic definitions of codecs.
In order to interoperate with rama, the generated topologies wrap EVERY stored value (in a pstate, depot, etc) as a `Tagged` value.

Wrapping everything obviously has overheads on serialization, time and memory usage. Ultimately I'd like to integrate more efficiently with rama.
One way this could work is by allowing users to provide explicit codecs or IDs in the DSL everywhere a serializable object is provided,
instead of rama's requirement to have the class itself inherit from RamaSerializable. This would be more efficient,
but the API would be cumbersome (useful as a compile target, but not for humans).

TODO: functions and their closures are serializable through some kind of magic. It might be useful to preemtively attempt to serialize all functions,
to flush out unserializable types with a more useful error message.
*/

class RamaBackend(setup: RamaModule.Setup, ramaTopologies: RamaModule.Topologies) {
	import RamaBackend._
	private val codecRegistry = new CodecRegistry()

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
			t.builder.vars.all.foreach { v =>
				codecRegistry.register(v.tag, v.codec)
			}
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
							compileIdentity(id), StreamSourceOptions.ackReturnAgg(compileCombiner(t.agg, t.tagR.tag))
						).out(compileVar(t.input))
							.`macro`(compiled)
							.ackReturn(compileVar(t.compiled._2))
					}

				case IdentityNamespace.Query =>
					codecRegistry.register(t.tagR, t.codec)
					val queryOutputVar = unsafeInternalVar(t.compiled._2.vid, "out", tag = t.tagR.tag)
					ramaTopologies.query(compileIdentity(id), compileVar(t.input.vid)).out(compileVar(queryOutputVar))
						.`macro`(compiled)
						.originPartition()
						.agg(rama.Agg.combiner(compileCombiner(t.agg, t.tagR.tag), compileVar(t.compiled._2.vid))).out(compileVar(queryOutputVar))

				case IdentityNamespace.PState => ()
			}
		}
	}

	private def compileNodes(nodes: List[Node]): Block.Impl =
		nodes.foldLeft(Block.create())((acc, n) => acc.`macro`(compileNode(n)))

	private def compileNode(node: Node): Block.Impl = node.op match {
		case Node.Dynamic(pos, ins, fn, op) => {
			// TODO: error reporting using the `pos`
			val inVars = ins.map(compileVar)
			def compileOp(op: DynamicOp): Block.Impl = op match {
				case DynamicOp.Map(out) => {
					val each = inVars match {
						case Nil => Block.each(() => out.tagAny(fn(Nil)))
						case List(a) => Block.each(a => out.tagAny(fn(List(untagAny(a)))), a)
						case List(a, b) => Block.each((a,b) => out.tagAny(fn(List(untagAny(a), untagAny(b)))), a, b)
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
						case List(a) => Block.each(new RamaOperation1[Tagged[Any]] {
							override def invoke(a: Tagged[Any], collector: OutputCollector) = {
								fn(List(a.untag)).asInstanceOf[Seq[Any]].foreach(x => collector.emit(out.tagAny(x)))
							}
						}, a)
						case other => throw new NotImplementedError(s"Explode[${other.length}]")
					}
					block.out(compileVar(out))
				}

				case DynamicOp.Filter(condVar) =>
					// map to a raw bool, then apply keepTrue
					val rawVar = unsafeInternalVar(condVar, "raw")
					compileOp(op = DynamicOp.Map(condVar)).`macro`(
						unwrapPrimitiveInto(condVar, rawVar)
					).keepTrue(compileVar(rawVar))
			}
			compileOp(op)
		}

		case Node.If(cond, ifBlock, elseBlock, _) => {
			val rawVar = unsafeInternalVar(cond, "raw")
			unwrapPrimitiveInto(cond, rawVar).`macro`(Block.ifTrue(
				compileVar(rawVar),
				Block.create().`macro`(compileNodes(ifBlock)),
				Block.create().`macro`(compileNodes(elseBlock)),
			))
		}

		case Node.Partition(in) => {
			val rawVar = unsafeInternalVar(in, "raw")
			unwrapPrimitiveInto(in, rawVar).hashPartition(compileVar(rawVar))
		}

		case Node.Assign(in, out) =>
			if (in === Var.unit.vid) {
				// TODO: is there any need to use Tagged here?
				Block.each(() => Tagged(())).out(compileVar(out))
			} else {
				Block.each(identity, compileVar(in)).out(compileVar(out))
			}

		case Node.Combine(block, agg, collectFrom, out) =>
			Block.create()
				.`macro`(compileNodes(block))
				.`macro`(Block.agg(Agg.combiner(compileCombiner(agg, out.tag), compileVar(collectFrom))).out(compileVar(out)))

		case Node.PStateUpdate(_pos, ps, path, ins, fn) =>
			val ramaPath = compilePath(path)
			// Rama models read-modify-write as a `term` on the path: the function
			// receives the current value (null when absent) and returns the new one.
			def wrapResult(newVal: Option[Any]) = newVal.map(Tagged.explicit(_, path.terminalTag)).orNull

			val mutation = ins match {
				case Nil => ramaPath.term(current => wrapResult(fn(Option(current).map(untagAny), Nil)))
				case List(a) => ramaPath.term((current, a) => wrapResult(fn(Option(current).map(untagAny), List(a))), compileVar(a))
				case List(a, b) => ramaPath.term((current, a, b) => wrapResult(fn(Option(current).map(untagAny), List(a, b))), compileVar(a), compileVar(b))
				case other => throw new NotImplementedError(s"PStateUpdate[${other.length}]")
			}
			Block.localTransform(compileIdentity(ps), mutation)

		case Node.PStateNode(psId, path, op) => {
			// The path tag reflects the intermediate / item type, whereas the returned Var is often a wrapper type (like List[T] or Option[T]).
			// Var types are automatically registered, ensure the raw item types are registered too.
			codecRegistry.registerRaw(path.terminalTag, path.codec)

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

				case PStateOp.Select(out: VarId, opts) =>
					val intermediate = unsafeInternalVar(out, "intermediate")
					val setPath = ramaPath // .nullToSet()
					val fullPath = opts match {
						case SelectOptions(Some(after), Some(limit), _) => setPath.sortedSetRangeFrom(compileVar(after), limit)
						case SelectOptions(None, Some(limit), _) => setPath.sortedSetRangeFromStart(limit)
						case SelectOptions(Some(after), None, _) => setPath.sortedSetRangeFrom(compileVar(after))
						case SelectOptions(None, None, _) => setPath
					}
					Block.localSelect(ps, fullPath)
						.out(compileVar(intermediate))
						// move the tagging from each element to the list itself
						.each((jlist: java.util.Collection[Any]) => out.tagAny(jlist.asScala.toList.map(untagAny)), compileVar(intermediate))
						.out(compileVar(out))

				case PStateOp.Get(out: VarId) => {
					val intermediate = unsafeInternalVar(out, "intermediate", tag = path.terminalTag)
					Block.localSelect(ps, ramaPath)
						.out(compileVar(intermediate))
						.each(v => intermediate.tagAny(Option(v).map(untagAny)), compileVar(intermediate))
						.out(compileVar(out))
				}

				case PStateOp.Put(v: VarId) => Block.localTransform(ps, ramaPath.termVal(compileVar(v)))
				// case PStateOp.Delete(v: VarId) => Block.localTransform(ps, ramaPath).termVoid()
			}
		}

		case Node.LocalAppend(dep, value) =>
			Block.depotPartitionAppend(compileIdentity(dep), compileVar(value))
	}
}

object RamaBackend {
	def apply(setup: RamaModule.Setup, topologies: RamaModule.Topologies) = new RamaBackend(setup, topologies)

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
		val p = Path.key(compilePathComponent(path.path.head))
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


	private def compileCombiner[O](agg: corama.Agg[O], tag: LightTypeTag): RamaCombinerAgg[Tagged[O]] = {
		val zero = agg.zero
		val combineFn = agg.combine
		new RamaCombinerAgg[Tagged[O]] {
			override def zeroVal(): Tagged[O] = Tagged.explicit(zero, tag)
			override def combine(current: Tagged[O], value: Tagged[O]): Tagged[O] = Tagged.explicit(combineFn(current.untag, value.untag), tag)
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
			case _: ClassSchema[?] => Left(classOf[Tagged[?]])

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

			case s: SetSchema[?] => {
				Right(applyIndexing(s, rama.PState.setSchema(s.v.tag.closestClass)))
			}
		}
	}

	private val untagAccessor: RamaFunction1[Tagged[Any], Any] = _.untag

	// used to get e.g. boolean from a Tagged[Boolean], for low level rama APIs which use a specific primitive type
	private def unwrapPrimitiveInto[A](source: VarId, dest: VarId): Block.Impl = {
		Block.each(untagAccessor, compileVar(source)).out(compileVar(dest))
	}
	
	// This is not a valid var in general, as the ID is not unique and the tag is not accurate.
	// It must only be used for:
	//  - local consumption (i.e. not cross-partition)
	//  - internal (i.e. not exposed as a var that user code can reference)
	private def unsafeInternalVar(base: VarId, suffix: String, tag: LightTypeTag = implicitly[Tag[Any]].tag) = {
		VarId(id = base.id, hint = s"${base.hint}|${suffix}", tag = tag)
	}
}
