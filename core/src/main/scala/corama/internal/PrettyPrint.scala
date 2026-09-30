package corama.internal

import corama.Topology
import izumi.reflect.Tag

object PrettyPrint {
	case class State(indent: Int) {
		def addIndent = copy(indent = indent + 1)
		def indentStr = "  " * indent
	}
	
	def format[S, R](topology: Topology[S, R]) = {
		lines(topology).mkString("\n")
	}

	private def cls[A](tag: Tag[A]): String = {
		cls(tag.closestClass)
	}

	private def cls[A](c: Class[A]): String = {
		c.getName()
	}

	private def simpleCls[A](c: Class[A]): String = {
		c.getSimpleName()
	}
	
	private def lines[S, R](topology: Topology[S, R]) = {
		s"# ${topology.id.value} from ${topology.builder.source.name}: ${cls(topology.builder.tag)}" ::
			s"  : returns ${cls(topology.compiled._2.tag)}" ::
			nodeLines(State(indent = 1), topology.compiled._1)
	}
	
	private def nodeLines(state: State, nodes: List[Node]): List[String] = {
		val indented = state.addIndent
		val i = state.indentStr
		nodes.flatMap { node =>
			var opStr = node.op.getClass().getSimpleName()

			val (out, tailLines) = node.op match {
				// non-branching nodes
				case n : Node.Dynamic => {
					opStr = simpleCls(n.op.getClass())
					import corama.internal.Node.DynamicOp.*
					n.op match {
						case Map(out) => (Some(out), Nil)
						case Explode(out) => (Some(out), Nil)
						case Filter(condVar) => (None, Nil)
					}
				}
				
				case n: Node.Assign => (Some(n.out), Nil)
				case n: Node.Partition => (None, Nil)

				case n: Node.PStateNode => {
					import corama.internal.Node.PStateOp.*
					opStr = simpleCls(n.op.getClass())
					n.op match {
						case _: Add => (None, Nil)
						case _: Remove =>(None, Nil)
						case Select(intermediate, out, options) => (Some(out), Nil)
						case Get(intermediate, out) => (Some(out), Nil)
						case _: Put => (None, Nil)
					}
				}
				
				case n: Node.PStateUpdate => (None, Nil)
				case n: Node.LocalAppend => (None, Nil)

				case Node.If(cond, ifBranch, elseBranch, branchOut) =>
					(
						Some(branchOut),
						((s"$i+ then:" ::
							nodeLines(indented, ifBranch)
						) ++ (s"$i+ else:" ::
							nodeLines(indented, elseBranch)
						))
					)

				case combine: Node.Combine =>
					(Some(combine.out), nodeLines(indented, combine.block))
			}

			val outStr = out.fold("")(out => s" => {${out.id}}")
			val depsList = node.op.deps.inputs.map(_.id).toList.sorted.mkString("{", ",", "}")
			val firstLine = s"$i* ${node.id.id}| ${opStr}: ${depsList}${outStr}"
			firstLine :: tailLines
		}
	}
}
