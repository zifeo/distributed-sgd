package epfl.distributed.data

import better.files._

object Dataset {

  val folder = "./data"

  type SparseVec = Map[Int, Double]

  def rcv1(): List[(SparseVec, String)] = {

    val dataFiles   = (0 until 0).map(d => s"lyrl2004_vectors_test_pt$d.data") + "lyrl2004_vectors_train.dat"
    val targetsFile = "rcv1-v2.topics.qrels"

    val targets = File(s"$folder/$targetsFile").lines.map { line =>
      val Array(cat, did, _) = line.split(' ')
      did -> cat
    }.toMap

    dataFiles
      .flatMap { file =>
        File(s"$folder/$file").lines
      }
      .map { line =>
        val Array(did, values @ _*) = line.split(' ')

        val weights = values.map { value =>
          val Array(idx, weight) = value.split(':')
          idx.toInt -> weight.toDouble
        }.toMap

        weights -> targets(did)
      }
      .toList
  }

}
