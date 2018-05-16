package epfl.distributed.math

import epfl.distributed.core
import scalapb.TypeMapper
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

  def foldLeft[B](init: B)(op: (B, Number) => B): B

  def sparse: Vec

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

  implicit val numberTypeMapper = TypeMapper[Double, Number](Number(_))(_.toDouble)

  implicit val typeMapper: TypeMapper[core.core.Sparse, Vec] =
    TypeMapper[core.core.Sparse, Vec](sparse => Vec(sparse.map, sparse.size))(vec =>
      core.core.Sparse(vec.map, vec.size))

  //Because nested messages (Sparse in our case) are serialized as Options in proto3, we provide those to make life easier
  implicit def toOptional(vec: Vec): Option[Vec]   = Option(vec)
  implicit def fromOptional(vec: Option[Vec]): Vec = vec.get

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

  def sum(vecs: Iterable[Vec]): Vec = {
    require(vecs.nonEmpty)
    val fst :: others = vecs
    require(others.forall(_.size == fst.size))
    others.foldLeft(fst)(_ + _)
  }
  def mean(vecs: Iterable[Vec]): Vec = sum(vecs) / vecs.size

  def randU[N <: Number: Uniform](size: Int, min: N, max: N): Dense                = Dense.randU(size, min, max)
  def randG[N <: Number: Gaussian](size: Int, mean: N = 0d, stdDev: N = 1d): Dense = Dense.randG(size, mean, stdDev)
  def randE[N <: Number: Exponential](size: Int, rate: N): Dense                   = Dense.randE(size, rate)

}
