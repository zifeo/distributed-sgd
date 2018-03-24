package epfl.distributed.core.ml

import epfl.distributed.data.dtypes.SparseVector

// need to transform into object
class SparseSVM[T <: SparseVector[T]](lambda: Double = 0) {

  def apply(w: T, x: T): Double =
    Math.max(1 - x.dot(w), 0)

  def gradient(w: T, x: T, y: Int): T = {
    val activity       = y * x.dot(w)
    val regularization = (w * (lambda * 2.0 / w.nonzero.toDouble)).sum
    val subgradient    = if (activity < 0) w.emptyLike else x * y
    subgradient + regularization
  }

}
