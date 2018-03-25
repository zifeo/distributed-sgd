package epfl.distributed.data

import spire.math._

case class F(f: Dense => Number) {
  import F.delta

  def apply(v: Dense): Number = f(v)

  def gradient(v: Dense): Dense = {
    Dense(
        v.v.zipWithIndex.map {
          case (xi, idx) =>
            val xiBigD = xi.toBigDecimal
            (f(Dense.oneHot(xiBigD + delta, v.size, idx)) - f(Dense.oneHot(xiBigD - delta, v.size, idx))) / (2 * delta)
        }
    )
  }
}

object F {

  private val delta: BigDecimal = BigDecimal(1e-25)

}
