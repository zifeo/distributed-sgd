package epfl.distributed.data

import spire.math._

case class F(f: Vec => Number) {
  import F.delta

  def apply(v: Vec): Number = f(v)

  def gradient(v: Vec): Vec = {
    Vec(
      v.zipWithIndex.map {
        case (xi, idx) =>
          val xiBigD = xi.toBigDecimal
          (f(Vec.oneHot(xiBigD + delta, v.size, idx)) - f(Vec.oneHot(xiBigD - delta, v.size, idx))) / (2 * delta)
      }
    )
  }
}

object F {

  private val delta: BigDecimal = BigDecimal(1e-25)

}
