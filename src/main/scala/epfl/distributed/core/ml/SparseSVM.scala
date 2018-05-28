package epfl.distributed.core.ml

import epfl.distributed.math.Vec
import spire.math._

/**
  * Sparse Support Vector Machine
  * @param lambda regularized parameter
  */
class SparseSVM(lambda: Double) {

  def apply(w: Vec, x: Vec): Number =
    forward(w, x)

  // compute the prediction
  def forward(w: Vec, x: Vec): Number =
    (1 - x.dot(w)) max 0

  // compute the gradient
  def backward(w: Vec, x: Vec, y: Int): Vec = {
    val activity       = y * x.dot(w)
    val nonZeroCount = w.nonZeroCount()
    val regularization = if (nonZeroCount > 0) ((lambda * 2.0 / nonZeroCount) * w).sum else Number.zero //This line induces an overhead of 700%
    val subgradient    = if (activity < 0) w.zerosLike else x * y

    subgradient + subgradient.valueLike(regularization)
  }

}
