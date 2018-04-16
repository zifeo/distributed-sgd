package epfl.distributed.core.ml

import epfl.distributed.math.Vec
import spire.math._

/**
  * Sparse Support Vector Machine
  * @param lambda regularized parameter
  */
class SparseSVM(lambda: Double = 0) {

  def apply(w: Vec, x: Vec): Number =
    forward(w, x)

  // compute the prediction
  def forward(w: Vec, x: Vec): Number =
    (1 - x.dot(w)) max 0

  // compute the gradient
  def backward(w: Vec, x: Vec, y: Int): Vec = {
    val activity       = y * x.dot(w)
    val regularization = if (w.nonZeroCount() > 0) (w * lambda * 2.0 / w.nonZeroCount().toDouble).sum else Number.zero
    val subgradient    = if (activity < 0) w.zerosLike else x * y
    subgradient + regularization
  }

}
