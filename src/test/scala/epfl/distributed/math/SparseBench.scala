package epfl.distributed.math

import epfl.distributed.utils.Dataset
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

object SparseBench extends Bench.LocalTime {

  override lazy val reporter = Reporter.Composite(
      LoggingReporter(),
      new RegressionReporter(
          RegressionReporter.Tester.OverlapIntervals(),
          RegressionReporter.Historian.ExponentialBackoff()),
      HtmlReporter(true)
  )

  val featuresCount = 47236

  //val data = Gen.enumeration("data")(Dataset.rcv1(30): _*)
  //val data = Dataset.rcv1("data", false)

  val sparseVectors = Gen.single[Array[Vec]]("Sparse")(Dataset.rcv1("data", false, vecFactory = Sparse.apply).map {
    case (x, _) => x
  })

  val sparseArrayVectors =
    Gen.single[Array[Vec]]("Sparse array")(Dataset.rcv1("data", false, vecFactory = SparseArrayVector.apply).map {
      case (x, _) => x
    })

  //val implementations = Gen.enumeration[Gen[Array[Vec]]]("Implementation")(sparseVectors, sparseArrayVectors)
  val implementations = Seq("Sparse" -> sparseVectors, "Sparse array" -> sparseArrayVectors)

  for ((name, impl) <- implementations) {
    performance of name in {
      measure method "addition" in {
        using(impl) in {
          _.reduceLeft[Vec](_ + _)
        }
      }

      measure method "product" in {
        using(impl) in {
          _.reduceLeft[Vec](_ * _)
        }
      }

      measure method "dot product" in {
        using(impl) in { vecs =>
          vecs.zip(vecs).map {
            case (v1, v2) => v1 dot v2
          }
        }
      }

      measure method "product by scalar" in {
        using(impl) in {
          _.map(_ * 2)
        }
      }

      measure method "norm squared" in {
        using(impl) in {
          _.map(_.normSquared)
        }
      }
    }
  }
}
