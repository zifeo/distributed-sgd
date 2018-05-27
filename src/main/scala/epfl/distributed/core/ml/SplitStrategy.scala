package epfl.distributed.core.ml

import epfl.distributed.utils.Dataset.Data

trait SplitStrategy {

  def apply(data: Data, nSlaves: Int): Iterable[Seq[Int]]

}

object SplitStrategy {

  val vanilla: SplitStrategy = (data: Data, nSlaves: Int) =>
    data.indices.grouped((data.length / nSlaves.toDouble).ceil.toInt).toSeq

}
