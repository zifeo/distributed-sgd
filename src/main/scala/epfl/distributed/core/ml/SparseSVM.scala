package epfl.distributed.core.ml

import epfl.distributed.math.Vec
import spire.math._

/**
  * Sparse Support Vector Machine
  * @param lambda regularized parameter
  */
class SparseSVM(val lambda: Double) {

  def apply(w: Vec, x: Vec): Number =
    forward(w, x)

  // compute the prediction
  def forward(w: Vec, x: Vec): Number =
    (1 - x.dot(w)) max 0

  // compute the gradient
  def backward(w: Vec, x: Vec, y: Int): Vec = {
    val activity       = y * x.dot(w)
    val subgradient    = if (activity < 0) w.zerosLike else x * y

    subgradient
  }

  def regularize(grad: Vec, w: Vec): Vec = grad + grad.valueLike(lambda * 2.0 * w.sum)
}
