package epfl.distributed.core.ml

import spire.math.Number

object EarlyStopping {
  type EarlyStopping = Seq[Number] => Boolean

  implicit val numberOrder: Ordering[Number] = (x: Number, y: Number) => x compare y

  def target(target: Number): EarlyStopping = (losses: Seq[Number]) => losses.last <= target

  def noImprovement(patience: Int = 1, minDelta: Number = 1e-3, minSteps: Option[Int] = None): EarlyStopping = {
    losses: Seq[Number] =>
      {
        def check: Boolean = {
          val (min, indexMin) = losses.zipWithIndex.minBy(_._1)

          if (indexMin == losses.size) {
            //The min is the last => still improving
            false
          }
          else {
            //The min is not the last => heck if we have been enough patient
            losses.size - indexMin - 1 >= patience
          }
        }

        minSteps.fold(check)(steps => if (steps < losses.size) false else check)
      }
  }

}
