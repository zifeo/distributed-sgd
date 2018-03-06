package epfl.distributed

import ml.dmlc.mxnet._

object SVM extends App {

  // usual:  mini_x \sum_{\alpha\in E} \max(1 - y_\alpha x^\top z_\alpha, 0) + \lambda || x ||_2^2
  // sparse: mini_x \sum_{\alpha\in E} (\max(1 - y_\alpha x^\top z_\alpha, 0) + \lambda \sum_{u\in e_\alpha} x_u^2/d_u)

  /*
  1: loop
  2:   Sample e uniformly at random from E
  3:   Read current state xe and evaluate Ge(x)
  4:   for v ∈ e do xv ← xv −γ b_v^top G_e(x)
  5: end loop
   */

  val x = NDArray.ones(1000, 10)
  val t = NDArray.ones(1000)
  val w = NDArray.ones(10)
  val lambda = 0f

  def forward(x: NDArray, t: NDArray, w: NDArray, lambda: Float): NDArray = {
    val y = NDArray.dot(x, w) * t
    NDArray.zeros(x.shape(0))
    NDArray.maximum(y * -1 + 1, 0) + NDArray.sum(NDArray.power(w, 2)) * lambda
  }

  def backward(x: NDArray, t: NDArray, w: NDArray, lambda: Double): NDArray = {
    //val y = t * w.t * x
    //val dn = 2 * lambda * w
    ???
  }

  def step(): NDArray =
    ???

  print(forward(x, t, w, lambda))

}
