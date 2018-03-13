package epfl.distributed

import spire.math._

case class F(f: Vec => Number) {
  import F.delta

  def apply(v: Vec): Number = f(v)

  def gradient(v: Vec): Vec = {
    Vec(
      v.map(_.toBigDecimal).zipWithIndex.map {
        case (xi, idx) =>
          (f(Vec.oneHot(xi + delta, v.size, idx)) - f(Vec.oneHot(xi - delta, v.size, idx))) / (2 * delta)
      }
    )
  }
}

object F {

  private val delta: BigDecimal = BigDecimal(1e-25)
}
