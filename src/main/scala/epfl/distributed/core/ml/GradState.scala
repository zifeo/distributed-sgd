package epfl.distributed.core.ml

import epfl.distributed.math.Vec

case class GradState(grad: Vec, start: Long = System.currentTimeMillis(), updates: Long = 0L, end: Option[Long] = None){

  def update(gradUpdate: Vec): GradState = copy(grad = grad - gradUpdate, updates = updates + 1)

  def finish: GradState = copy(end = Some(System.currentTimeMillis()))
}

object GradState {

  def empty: GradState = GradState(Vec.zeros(1), start = 0)
}