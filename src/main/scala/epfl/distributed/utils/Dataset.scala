package epfl.distributed.utils

import epfl.distributed.math.Vec
import spire.math.Number

import scala.io.Source

object Dataset {

  type Data = Array[(Vec, Int)]

  // CCAT transform to -1 or 1
  def rcv1(folder: String, full: Boolean = true, chunk: Int = 4096, featuresCount: Int = 47236): Data = {

    def readData(path: String, chunk: Int): Iterator[(Int, Map[Int, Number])] =
      for {
        lines <- Source.fromFile(path).getLines.grouped(chunk)
        line  <- lines.par
      } yield {
        val parts = line.split(' ')
        val rowID = parts(0).toInt
        val vec = parts
          .drop(2)
          .map { row =>
            val elems = row.split(':')
            elems(0).toInt -> Number(elems(1).toDouble)
          }
          .toMap
        (rowID, vec)
      }

    def readLabels(path: String, chunk: Int): Iterator[(Int, Int)] =
      for {
        lines <- Source.fromFile(path).getLines.grouped(chunk)
        line  <- lines.par
      } yield {
        val parts      = line.split(' ')
        val classLabel = parts(0)
        val rowID      = parts(1)
        (rowID.toInt, if (classLabel == "CCAT") 1 else -1)
      }

    val dataFiles = s"$folder/lyrl2004_vectors_train.dat" +: (if (full)
                                                                (0 to 3).map(d =>
                                                                  s"$folder/lyrl2004_vectors_test_pt$d.dat")
                                                              else List.empty)
    val targetsFile = s"$folder/rcv1-v2.topics.qrels"

    val labels = readLabels(targetsFile, chunk).toMap

    for {
      rows <- dataFiles.map(f => readData(f, chunk)).toArray
      (id, v)  <- rows
    } yield (Vec(v, featuresCount), labels(id))
  }

}
