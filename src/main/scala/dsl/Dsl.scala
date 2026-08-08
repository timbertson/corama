package dsl

import com.rpl.rama
import com.rpl.rama._
import com.rpl.rama.module.StreamTopology
import com.rpl.rama.ops._
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Help
import dsl.internal.*

type BlockM[A] = dsl.internal.BlockM[A]
type JList[A] = java.util.List[A]
type JLong = java.lang.Long
type JInt = java.lang.Integer

class Var[A](val raw: String) {
	def map[B](fn: A => B) = each(this, fn)
	def foreach[B](fn: A => B) = each_(this, fn)
}

object Var {
	def nothing: Var[Nothing] = new Var("*__error_nothing_var_referenced")

	given RamaTerm[Var[?]] with {
		def ramaTerm(a: Var[?]): Object = a.raw
	}
	
	def depot(name: String): Var[Depot] = new Var[Depot]("*" + name)
	def pstate(name: String): Var[PState] = new Var[PState]("$$" + name)
	// def v(name: String): Binding[V] = new Binding[V]("*" + name)
}

object Expr {
	// def eq[A, B](a: A, b: B)(implicit termA: RamaTerm[A], termB: RamaTerm[B]) = new rama.Expr(Ops.EQUAL, termA.ramaTerm(a), termB.ramaTerm(b))
	def eq[A](a: Var[A], b: A) = new rama.Expr(Ops.EQUAL, a.raw, b)
	def eqVars[A](a: Var[A], b: Var[A]) = new rama.Expr(Ops.EQUAL, a.raw, b.raw)
	def notNull[A](a: Var[A]) = new rama.Expr(Ops.IS_NOT_NULL, a.raw)
	def isNull[A](a: Var[A]) = new rama.Expr(Ops.IS_NULL, a.raw)
	
	extension (e: rama.Expr) {
		def and(other: rama.Expr) = new rama.Expr(Ops.AND, e, other)
		def or(other: rama.Expr) = new rama.Expr(Ops.OR, e, other)
	}
}

class Path(val underlying: rama.Path.Impl) {
	def key[A](k: A)(implicit term: RamaTerm[A]) = new Path(underlying.key(term.ramaTerm))

	def sortedMapRangeFrom[A](minRange: A, maxAmt: Int, inclusive: Boolean = true)(implicit term: RamaTerm[A]) = {
		var opts = SortedRangeFromOptions.maxAmt(maxAmt)
		if (inclusive) {
			opts = opts.excludeStart()
		}
		new Path(underlying.sortedMapRangeFrom(term.ramaTerm(minRange), opts))
	}

	def sortedSetRangeFrom[A](minRange: A, maxAmt: Int, inclusive: Boolean = true)(implicit term: RamaTerm[A]) = {
		var opts = SortedRangeFromOptions.maxAmt(maxAmt)
		if (inclusive) {
			opts = opts.excludeStart()
		}
		new Path(underlying.sortedSetRangeFrom(term.ramaTerm(minRange), opts))
	}
	
	def mapVals = new Path(underlying.mapVals())
	
	def termVal[A](a: A)(implicit term: RamaTerm[A]) = new Path(underlying.termVal(term.ramaTerm(a)))

	def term[A, B, C](fn: RamaFunction2[B, A, B], a: A)(implicit term: RamaTerm[A]) = new Path(underlying.term(fn, term.ramaTerm(a)))
	def term[A](fn: RamaFunction1[A, A]) = new Path(underlying.term(fn))
	
	def nullToVal[A](a: A)(implicit term: RamaTerm[A]) =
		new Path(underlying.nullToVal(term.ramaTerm(a)))
}

object Path {
	def create = new Path(rama.Path.create())
	def key[A](k: A)(implicit term: RamaTerm[A]) = create.key(k)
	def sortedMapRangeFrom[A](minRange: A, maxAmt: Int, inclusive: Boolean = true)(implicit term: RamaTerm[A]) =
		create.sortedMapRangeFrom(minRange, maxAmt, inclusive = inclusive)

	def sortedSetRangeFrom[A](minRange: A, maxAmt: Int, inclusive: Boolean = true)(implicit term: RamaTerm[A]) =
		create.sortedSetRangeFrom(minRange, maxAmt, inclusive = inclusive)
}

extension (s: StreamTopology) {
	def sourceBlock[T](binding: Var[Depot])(build: Var[T] => BlockM[Nothing]): Unit = {
		val v = new Var[T](Helpers.genVar(binding.raw.stripPrefix("*")))
		s.source(binding.raw).out(v.raw).`macro`(build(v).block)
		()
	}
}

implicit def coerceRamaFn1[A,B](f: A => B): RamaFunction1[A, B] = new RamaFunction1[A, B] {
	override def invoke(a: A): B = f(a)
}

implicit def coerceRamaFn2[A,B,C](f: (A, B) => C): RamaFunction2[A, B, C] = new RamaFunction2[A, B, C] {
	override def invoke(a: A, b: B): C = f(a, b)
}

def each[V1, O](v: Var[V1], fn: V1 => O, varName: String = null): BlockM[O] = {
	BlockM.genOut(Block.each(fn, v.raw), Option(varName).getOrElse(s"from-${v.raw}"))
}

def each_[V1](v: Var[V1], fn: V1 => Any): BlockM[Nothing] = {
	lift(Block.each(fn, v.raw))
}

def allPartition = lift(Block.allPartition())
def shufflePartition = lift(Block.shufflePartition())
def hashPartition(v: Var[?]) = lift(Block.hashPartition(v.raw))

def lift(block: Block.Impl) = BlockM.effect(block)
def keepTrue(arg: rama.Expr) = BlockM.effect(Block.keepTrue(arg))

def ifTrue(arg: rama.Expr, thenDo: BlockM[?]) =
	BlockM.effect(Block.ifTrue(arg, thenDo.block))

def ifTrue(arg: rama.Expr, thenDo: BlockM[?], elseDo: BlockM[?]) =
	BlockM.effect(Block.ifTrue(arg, thenDo.block, elseDo.block))

def localSelect[T](pstate: Var[PState], path: Path, varName: String = null) =
	BlockM.genOut[T](Block.localSelect(pstate.raw, path.underlying), Option(varName).getOrElse(s"select-${pstate.raw}"))

def localSelect_(pstate: Var[PState], path: Path) = lift(Block.localSelect(pstate.raw, path.underlying))

def localTransform[T](pstate: Var[PState], path: Path) = lift(Block.localTransform(pstate.raw, path.underlying))

def caseExpr(expr: rama.Expr)(block: BlockM[?]) = Case.create(expr, block.block)
def defaultCase(block: BlockM[?]) = Case.create(true).`macro`(block.block)
def cond(cases: Case*) = lift(Block.cond(cases*))
def explode[T](v: Var[?], varName: String = null) = BlockM.genOut[T](Block.each(Ops.EXPLODE, v), Option(varName).getOrElse(s"explode-${v.raw}"))
def explodeList[A](v: Var[JList[A]], varName: String = null) = BlockM.genOut[A](Block.each(Ops.EXPLODE, v), Option(varName).getOrElse(s"explode-${v.raw}"))
