package epfl.distributed.math

import spire.math.{Number, Numeric, sqrt}
import spire.random.{Exponential, Gaussian, Uniform}

import scala.language.implicitConversions

trait Vec {

  def map: Map[Int, Number]

  def values: Iterable[Number] = map.values

  require(!values.exists(_ == Double.NaN), "Trying to build a Vec with a NaN value")

  def size: Int

  def elementWiseOp(other: Vec, op: (Number, Number) => Number): Vec

  def mapValues(op: Number => Number): Vec

  def reduce(op: (Number, Number) => Number): Number = values.reduce(op)

  def fold(init: Number)(op: (Number, Number) => Number): Number = values.fold(init)(op)

  def foldLeft[B](init: B)(op: (B, Number) => B): B = values.foldLeft(init)(op)

  def sparse: Vec

  def apply(idx: Int): Number

  def +(other: Vec): Vec = elementWiseOp(other, _ + _)

  def +(scalar: Number): Vec = if (scalar === Number.zero) this else mapValues(_ + scalar)

  def -(other: Vec): Vec = elementWiseOp(other, _ - _)

  def -(scalar: Number): Vec = if (scalar === Number.zero) this else mapValues(_ - scalar)

  def *(other: Vec): Vec = elementWiseOp(other, _ * _)

  def *(scalar: Number): Vec = if (scalar === Number.zero) this.zerosLike else mapValues(_ * scalar)

  def /(other: Vec): Vec = elementWiseOp(other, _ / _)

  def /(scalar: Number): Vec =
    if (scalar === Number.zero) throw new IllegalArgumentException("Division by zero") else mapValues(_ / scalar)

  def **(scalar: Number): Vec = mapValues(_ ** scalar)

  def unary_- : Vec = mapValues(-_)

  def sum: Number = fold(Number.zero)(_ + _)

  def normSquared: Number = foldLeft(Number.zero)(_ + _ ** 2)
  def norm: Number        = sqrt(normSquared)

  def dot(other: Vec): Number = (this * other).sum

  def zerosLike: Vec = this match {
    case _: Dense  => Dense.zeros(size)
    case _: Sparse => Sparse.zeros(size)
  }

  def valueLike(value: Number): Vec = {
    if (value === Number.zero) {
      zerosLike
    }
    else {
      this match {
        case _: Dense  => Dense.fill(value, size)
        case _: Sparse => Sparse(map.mapValues(_ => value), size)
      }
    }
  }

  def onesLike: Vec = valueLike(Number.one)

  def sparsity(epsilon: Number = 1e-20): Double = 1 - nonZeroCount(epsilon).toDouble / size

  def nonZeroCount(epsilon: Number = 1e-20): Int
  //def nonZeroCount: Int = nonZeroCount()

  def nonZeroIndices(epsilon: Number = 1e-20): Iterable[Int]
}

object Vec {

  implicit class RichNumber(val n: Number) extends AnyVal {

    def *(vec: Vec): Vec = {
      vec * n
    }
  }
  implicit class RichInt(val n: Int) extends AnyVal {

    def *(vec: Vec): Vec = {
      vec * n
    }
  }
  implicit class RichDouble(val n: Double) extends AnyVal {

    def *(vec: Vec): Vec = {
      vec * n
    }
  }

  def apply(numbers: Number*): Dense          = Dense(numbers.toVector)
  def apply(numbers: Iterable[Number]): Dense = Dense(numbers.toVector)

  def apply(size: Int, values: (Int, Number)*): Sparse     = Sparse(values.toMap, size)
  def apply(values: Map[Int, Number], size: Int): Sparse   = Sparse(values, size)
  def apply[A: Numeric](m: Map[Int, A], size: Int): Sparse = Sparse(m, size)

  def zeros(size: Int): Vec               = Sparse.zeros(size)
  def ones(size: Int): Vec                = Dense.ones(size)
  def fill(value: Number, size: Int): Vec = Dense.fill(value, size)

  def oneHot(value: Number, index: Int, size: Int): Vec = {
    if (value === Number.zero) {
      zeros(size)
    }
    else {
      Dense.oneHot(value, index, size)
    }
  }

  def sum(vecs: Iterable[Vec]): Vec = {
    require(vecs.nonEmpty, "Cannot sum an empty list of vectors")
    vecs.reduce(_ + _)
  }

  def sparseSum(vecs: Iterable[Vec]): Vec = {
    require(vecs.nonEmpty, "Cannot sum an empty list of vectors")
    val keysets = vecs.foldLeft(Set.empty[Int])(_ union _.map.keySet)
    Sparse(keysets.map(idx => idx -> vecs.foldLeft(Number.zero)(_ + _(idx))).toMap, vecs.head.size)
  }

  def mean(vecs: Iterable[Vec]): Vec = sum(vecs) / vecs.size

  def randU[N <: Number: Uniform](size: Int, min: N, max: N): Dense                = Dense.randU(size, min, max)
  def randE[N <: Number: Exponential](size: Int, rate: N): Dense                   = Dense.randE(size, rate)
  def randG[N <: Number: Gaussian](size: Int, mean: N = 0d, stdDev: N = 1d): Dense = Dense.randG(size, mean, stdDev)

}
