package epfl.distributed.data.dtypes

trait SparseVector[T <: SparseVector[T]] { self: T =>

  def nonzero: Int

  def emptyLike: T

  def +(b: Double): T

  def +(bs: T): T

  def *(b: Double): T

  def dot(bs: T): Double

  def sum: Double

}
