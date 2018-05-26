package epfl.distributed.core.ml

import spire.math.Number

object EarlyStopping {

  type EarlyStopping = Seq[Number] => Boolean

  implicit val numberOrder: Ordering[Number] = (x: Number, y: Number) => x compare y

  def target(target: Number): EarlyStopping = (losses: Seq[Number]) => losses.headOption.fold(false)(_ <= target)

  def noImprovement(patience: Int = 7, minDelta: Number = 1e-3, minSteps: Option[Int] = None): EarlyStopping = {
    losses: Seq[Number] =>
      {
        def check: Boolean = {
          val (_, indexMin) = losses.zipWithIndex.minBy(_._1)

          if (indexMin == 0) {
            //The min is the latest => still improving
            false
          }
          else {
            //The min is not the latest => check if we have been enough patient
            indexMin >= patience
            //losses.size - indexMin - 1 >= patience
          }
        }

        losses.nonEmpty && minSteps.fold(check)(steps => if (steps < losses.size) false else check)
      }
  }

}
