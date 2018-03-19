package epfl.distributed.data

import better.files._

object Dataset {

  val folder = "./data"

  type SparseVec = Map[Int, Float]

  def rcv1(): Vector[(SparseVec, String)] = {

    val dataFiles   = (0 to 3).map(d => s"lyrl2004_vectors_test_pt$d.dat") :+ "lyrl2004_vectors_train.dat"
    val targetsFile = "rcv1-v2.topics.qrels"

    val targets = File(s"$folder/$targetsFile").lines.map { line =>
      val Array(cat, did, _) = line.split(' ')
      did -> cat
    }.toMap

    dataFiles.toVector
      .flatMap { file =>
        println(file)
        File(s"$folder/$file").lineIterator
          .map { line =>
            val Array(did, values @ _*) = line.split(' ')

            val weights = values.collect {
              case value if value.nonEmpty =>
                val Array(idx, weight) = value.split(':')
                idx.toInt -> weight.toFloat
            }.toMap

            weights -> targets(did)
          }
      }
  }

}
