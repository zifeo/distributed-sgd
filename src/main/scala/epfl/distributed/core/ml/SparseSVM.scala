package epfl.distributed.core.ml

import epfl.distributed.math.Vec
import spire.math._
import spire.math.Number.{zero, one}

/**
  * Sparse Support Vector Machine
  * @param lambda regularized parameter
  */
class SparseSVM(val lambda: Number, val dimSparsity: Vec) {

  // compute the prediction
  def forward(w: Vec, x: Vec): Number = (x dot w).signum * -one

  def loss(pred: Number, y: Int): Number = max(zero, one - y * pred)

  def loss(w: Vec, x: Vec, y: Int): Number = loss(forward(w, x), y)

  def loss(w: Vec, samples: Iterable[(Vec, Int)]): Number =
    lambda * w.normSquared + samples
      .map { case (x, y) => loss(w, x, y) }
      .reduce(_ + _) / samples.size

  def predictLabel(w: Vec, x: Vec): Int = if (forward(w, x) >= zero) 1 else -1

  // compute the gradient
  def backward(w: Vec, x: Vec, y: Int): Vec = {
    val activity = y * x.dot(w)
    if (activity < 0) w.zerosLike else x * y
  }

  def regularize(grad: Vec, w: Vec): Vec = grad + grad.valueLike(lambda * 2.0 * w.dot(dimSparsity))

}
