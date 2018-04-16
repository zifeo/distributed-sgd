package epfl.distributed.math

import spire.math.{Number, Numeric, sqrt}
import spire.random.{Exponential, Gaussian, Uniform}

trait Vec {

  def size: Int

  def elementWiseOp(other: Vec, op: (Number, Number) => Number): Vec

  def mapValues(op: Number => Number): Vec

  def foldLeft[B](init: B)(op: (B, Number) => B): B

  def sparse: Vec

  def map: Map[Int, Number]

  def +(other: Vec): Vec = elementWiseOp(other, _ + _)

  def +(scalar: Number): Vec = mapValues(_ + scalar)

  def -(other: Vec): Vec = elementWiseOp(other, _ - _)

  def -(scalar: Number): Vec = mapValues(_ - scalar)

  def *(other: Vec): Vec = elementWiseOp(other, _ * _)

  def *(scalar: Number): Vec = mapValues(_ * scalar)

  def /(other: Vec): Vec = elementWiseOp(other, _ / _)

  def /(scalar: Number): Vec = mapValues(_ / scalar)

  def **(scalar: Number): Vec = mapValues(_ ** scalar)

  def unary_- : Vec = mapValues(-_)

  def sum: Number = foldLeft(Number.zero)(_ + _)

  def normSquared: Number = foldLeft(Number.zero)(_ + _ ** 2)
  def norm: Number        = sqrt(normSquared)

  def dot(other: Vec): Number = {
    require(other.size == size, "Can't perform dot product of vectors of different length")

    (this * other).sum
  }

  def zerosLike: Vec = this match {
    case _: Dense  => Dense.zeros(this.size)
    case _: Sparse => Sparse.zeros(this.size)
  }

  def sparsity(epsilon: Number = 1e-20): Double = 1 - nonZeroCount(epsilon).toDouble / size

  def nonZeroCount(epsilon: Number = 1e-20): Int
  //def nonZeroCount: Int = nonZeroCount()

  def nonZeroIndices(epsilon: Number = 1e-20): Iterable[Int]
}

object Vec {

  def apply(numbers: Number*): Dense          = Dense(numbers.toVector)
  def apply(numbers: Iterable[Number]): Dense = Dense(numbers.toVector)

  def apply(size: Int, values: (Int, Number)*): Sparse   = Sparse(values.toMap, size)
  def apply(values: Map[Int, Number], size: Int): Sparse = Sparse(values, size)

  def apply[A: Numeric](m: Map[Int, A], size: Int): Sparse = {
    val num = implicitly[Numeric[A]]
    Sparse(m.mapValues(num.toNumber), size)
  }

  def zeros(size: Int): Vec                               = Sparse.zeros(size)
  def ones(size: Int): Vec                                = Dense.ones(size)
  def fill(value: Number, size: Int): Dense               = Dense.fill(value, size)
  def oneHot(value: Number, index: Int, size: Int): Dense = Dense.oneHot(value, index, size)

  def randU[N <: Number: Uniform](size: Int, min: N, max: N): Dense                = Dense.randU(size, min, max)
  def randG[N <: Number: Gaussian](size: Int, mean: N = 0d, stdDev: N = 1d): Dense = Dense.randG(size, mean, stdDev)
  def randE[N <: Number: Exponential](size: Int, rate: N): Dense                   = Dense.randE(size, rate)

}
