package epfl.distributed.core.ml

import epfl.distributed.math.Vec
import spire.math.Number

case class GradState(grad: Vec, loss: Option[Number], start: Long, updates: Long, end: Option[Long]) {

  def update(gradUpdate: Vec): GradState = copy(grad = grad - gradUpdate, updates = updates + 1)

  def replaceGrad(newGrad: Vec): GradState = copy(grad = newGrad, updates = updates + 1)

  def finish(finalLoss: Number): GradState = copy(loss = Some(finalLoss), end = Some(System.currentTimeMillis()))
}

object GradState {

  def empty: GradState = GradState(Vec.zeros(1), loss = None, start = 0, updates = 0, end = Some(0))

  def start(grad: Vec,
            loss: Option[Number] = None,
            start: Long = System.currentTimeMillis(),
            updates: Long = 0L,
            end: Option[Long] = None): GradState = GradState(grad, loss, start, updates, end)
}
