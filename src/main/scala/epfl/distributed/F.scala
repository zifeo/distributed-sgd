package epfl.distributed

import spire.math._

case class F(f: Vec => Number) {
  import F.delta

  def apply(v: Vec): Number = f(v)

  def gradient(v: Vec): Vec = {
    Vec(v.map(xi => (f(xi + delta) + f(xi - delta)) / 2 * delta))
  }
}

object F{

  private def delta = 1e-15
}
