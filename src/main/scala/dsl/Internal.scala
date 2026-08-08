package dsl.internal

import dsl.*
import com.rpl.rama
import com.rpl.rama._
import com.rpl.rama.module.StreamTopology
import com.rpl.rama.ops._
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Help


class BlockM[A](val block: Block.Impl, private val v: Var[A]) {
	def map(fn: Var[A] => Var[Nothing]): BlockM[A] = {
		// Note: map is a bit of a lie, should only be used to yield nothing
		this
		// new BlockM[B](block, fn(v))
	}

	def flatMap[B](fn: Var[A] => BlockM[B]): BlockM[B] = {
		val next = fn(v)
		// chaining is a bit weird, but eh...
		BlockM(block.`macro`(next.block), next.v)
	}
	
}

object BlockM {
	def pure[A](v: Var[A]): BlockM[A] = {
		new BlockM(Block.create(), v)
	}
	
	def genOut[A](block: Block.OutImpl, prefix: String = null) = {
		val outV = new Var[A](
			Option(prefix).fold(Helpers.genVar())(Helpers.genVar)
		)
		new BlockM[A](block.out(outV.raw), outV)
	}
	
	def effect(block: Block.Impl) = {
		new BlockM[Nothing](block, Var.nothing)
	}
}

// RamaTerm: merge special types and regular objects into the same type space.
// Currently handles:
// Var (into string)
// Expr (self)
trait RamaTerm[A] {
	def ramaTerm(a: A): Object
}

trait LowPriorityRamaTerm {
	given defaultRamaTerm[A <: Object]: RamaTerm[A] with {
		def ramaTerm(a: A): Object = a
	}
}

object RamaTerm extends LowPriorityRamaTerm
