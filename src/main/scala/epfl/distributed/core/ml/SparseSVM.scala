package epfl.distributed.core.ml

import epfl.distributed.data.Vec
import spire.math._

class SparseSVM(lambda: Double = 0) {

  def apply(w: Vec, x: Vec): Number =
    (1 - x.dot(w)) max 0

  def gradient(w: Vec, x: Vec, y: Int): Vec = {
    val activity       = y * x.dot(w)
    val regularization = (w * (lambda * 2.0 / w.nonZeroCount().toDouble)).sum
    val subgradient    = if (activity < 0) w.zerosLike else x * y
    subgradient + regularization
  }

}
