package epfl.distributed.data

import better.files.File
import epfl.distributed.Main.Data
import kantan.codecs.resource.ResourceIterator
import kantan.csv.ops._
import kantan.csv._

object Dataset {

  val folder = "./data"

  // CCAT transform to -1 or 1
  def rcv1(entries: Int = 10): Data = {
    // not that safe

    val dataFiles   = (0 to 3).map(d => s"lyrl2004_vectors_test_pt$d.dat") :+ "lyrl2004_vectors_train.dat"
    val targetsFile = "rcv1-v2.topics.qrels"

    def dataReader(filename: String): CsvReader[ReadResult[Vector[String]]] = {
      val url = File(s"$folder/$filename").url
      url.asCsvReader[Vector[String]](rfc.withCellSeparator(' '))
    }

    val targets = dataReader(targetsFile)
      .map { line =>
        line.map { cells =>
          val Vector(cat, did, _) = cells
          val label               = if (cat == "CCAT") 1 else -1
          did -> label
        }.toOption

      }
      .flatten
      .toMap

    val data =
      ResourceIterator(dataFiles: _*)
        .flatMap(dataReader)
        .map { line =>
          line.map { cells =>
            val did    = cells.head
            val values = cells.tail

            val weights = values
              .filter(_.nonEmpty)
              .map { value =>
                val Array(idx, weight) = value.split(':')
                idx.toInt -> weight.toDouble
              }
              .toMap

            weights -> targets(did)
          }.toOption
        }
        .take(entries)
        .flatten
        .toArray

    data
  }

}
