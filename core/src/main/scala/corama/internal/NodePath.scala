package corama.internal

import scala.collection.immutable.Queue
import scala.util.*
import corama.error.Util.*
import corama.*
import corama.internal.*

sealed trait PathIndex

case class NodeIndex(value: Int) extends PathIndex {
	def next: NodeIndex = NodeIndex(value + 1)
	def child(branch: Int): NodeBranch = NodeBranch(value, branch)
}

object NodeIndex {
	val zero = NodeIndex(0)
}

case class NodeBranch(step: Int, branch: Int) extends PathIndex {
	def stepIndex: NodeIndex = NodeIndex(step)
	def raw: List[Int] = List(step, branch)
}

case class NodePath(branches: Queue[NodeBranch], leaf: NodeIndex) {
	def raw: Seq[Int] = branches.flatMap(_.raw).appended(leaf.value)
	def rawString: String = raw.mkString(".")
}

object NodePath {
	def root = NodePath(Queue.empty, NodeIndex.zero)
	def fromRaw(path: List[Int]): Try[NodePath] = {
		path match {
			case p0 :: p1 :: path => {
				// nested node branch
				fromRaw(path).map { inner =>
					NodePath(inner.branches.appended(NodeBranch(p0, p1)), inner.leaf)
				}
			}

			case p0 :: Nil => {
				Success(NodePath(Queue.empty, NodeIndex(p0)))
			}

			case Nil => Failure(err("Not enough path components"))
		}
	}
}

// all frames other than the current have a NodeBranch, the current frame
// is represented explicitly in ExecutionStack
case class ExecutionFrame(index: NodeIndex, remainingNodes: List[Node])

// First element in stack is the innermost frame, last element is the root frame
// The frame index is where we _are_, but note that `remainingNodes` has an index
case class ExecutionStack(index: NodeIndex, remainingNodes: List[Node], stack: List[(NodeBranch, ExecutionFrame)]) {
	def head = remainingNodes.headOption

	def next: Option[(Node, ExecutionStack)] = remainingNodes match {
		case head :: tail =>
			Some((head, copy(index = index.next, remainingNodes = tail)))
		case Nil => None
	}

	private def skip(n: Int): Option[ExecutionStack] = {
		remainingNodes.drop(n) match {
			case Nil => None
			case remaining => {
				val newIndex = NodeIndex(index.value + n)
				Some(copy(index = newIndex, remainingNodes = remaining))
			}
		}
	}
	
	private def enterBranchAtHead(branch: Int): Try[ExecutionStack] = {
		???
		// head.toRight(err(s"Not enough steps")).toTry.flatMap {
		// 	case step: FoldEitherNode[?, ?, ?] => branch match {
		// 		case 0 => Success(replaceWithChildBranch(0, step.left.toList))
		// 		case 1 => Success(replaceWithChildBranch(1, step.right.toList))
		// 		case other => Failure(new RuntimeException(s"${branch}: attempted to resume FoldEitherNode[$other]"))
		// 	}
		// 	case other => Failure(new RuntimeException(s"${branch}: attempted to continue within step ${other}"))
		// }
	}

	def enterBranch(branch: NodeBranch): Try[ExecutionStack] = {
		skip(branch.step).toRight(err(s"${branch} Not enough steps")).toTry
			.flatMap(_.enterBranchAtHead(branch.branch))
	}

	def numRemainingNodes = remainingNodes.length
	def path: NodePath = NodePath(Queue.from(stack.map(_._1).reverse), index)

	def replaceWithChildBranch(branch: Int, steps: List[Node]) = {
		val newStack = (this.index.child(branch), ExecutionFrame(this.index.next, this.remainingNodes.tail)) :: stack
		ExecutionStack(NodeIndex(0), steps, newStack)
	}

	def pop: Option[ExecutionStack] = {
		stack match {
			case Nil => None
			case (_branch, frame) :: tail =>
				Some(ExecutionStack(frame.index, frame.remainingNodes, tail))
		}
	}

	// detailed graph-like printing
	def summaryString = summaryLines.mkString("\n")
	def summaryLines: List[String] = {
		def formatNodes(pathPrefix: Seq[Int], stepIndex: Int, steps: List[Node]): List[String] = {
			val indent = "  " * pathPrefix.length
			steps.zipWithIndex.flatMap { (step, relativeIndex) =>
				val stepi = stepIndex + relativeIndex
				val path = pathPrefix.appended(stepi)
				val title = s"@${path.mkString(".").padTo(16, ' ')}| ${indent}${step}"
				
				val childLines : List[String] = ???
				// 	step.children.zipWithIndex.flatMap { (child, branch) =>
				// 	formatNodes(path.appended(branch), 0, child.toList)
				// }
				title :: childLines
			}
		}
		val prefix = this.path.branches.flatMap(_.raw)
		val thisLevel = formatNodes(prefix, this.path.leaf.value, this.remainingNodes)
		thisLevel ++ this.pop.toList.flatMap(_.summaryLines)
	}
	
	def deps: DependencySet = {
		val thisLevel = DependencySet.empty.addAll(remainingNodes)
		this.pop match {
			case None => thisLevel
			case Some(parentStack) => {
				val parentDeps = parentStack.deps
				DependencySet(
					// allow this level's produces to satisfy parent stack
					// requires (an Assign step propagates the inner branch's
					// result to the parent, and other provides should be harmless
					// since the parent should have no way of accessing them)
					requires = parentDeps.requires.diff(thisLevel.produces).union(thisLevel.requires),
					
					// only let branch produces propagate one level up
					produces = parentDeps.produces
				)
			}
		}
	}
}

object ExecutionStack {
	def root(steps: List[Node]): ExecutionStack = ExecutionStack(NodeIndex(0), steps, Nil)
}

case class DependencySet(requires: Set[VarId], produces: Set[VarId]) {
	def add(step: Node) = {
		val newProduces = produces.union(step.deps.output.toSet)

		// we accumulate child requirements but not depends (those don't escape their scope)
		val childRequirements = step.children.flatMap { steps =>
			this.addAll(steps.toList).requires
		}.toSet

		val newRequires = requires.union(step.deps.inputs.toSet).union(childRequirements)

		DependencySet(requires = newRequires.diff(produces), produces = newProduces)
	}
	
	def addAll(steps: List[Node]): DependencySet = {
		steps match {
			case Nil => this
			case step :: steps => this.add(step).addAll(steps)
		}
	}
}

object DependencySet {
	def empty = DependencySet(Set.empty, Set.empty)
}

