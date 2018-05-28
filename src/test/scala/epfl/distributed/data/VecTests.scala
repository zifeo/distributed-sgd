package epfl.distributed.data

import epfl.distributed.Spec
import epfl.distributed.math.{Dense, Sparse, Vec}

class VecTests extends Spec {

  "Vec" when {

    "being used" should {

      "work for dense" in {

        val v1 = Vec(1, 2, 3)
        val v2 = Vec(1, 2, 3)

        v1 + v2 shouldBe Vec(2, 4, 6)
        v1 dot v2 shouldBe (1 + 4 + 9)
        v1 * 2 shouldBe Vec(2, 4, 6)
        3 * v1 shouldBe Vec(3, 6, 9)
        v1.norm shouldBe Math.sqrt(1 + 4 + 9)

      }

      "work for sparse" in {
        val v1 = Sparse(Map(0 -> 1, 1 -> 2, 2 -> 3), 4)
        val v2 = Sparse(Map(1 -> 1, 2 -> 2, 3 -> 3), 4)

        v1 + v2 shouldBe Sparse(Map(0 -> 1, 1 -> 3, 2 -> 5, 3 -> 3), 4)
      }

      "raise an error in case of NaN" in {
        an [IllegalArgumentException] should be thrownBy Sparse(Map(0 -> 1, 1 -> 2, 2 -> 3), 4) / 0
        an [IllegalArgumentException] should be thrownBy Dense(1, 2, 3, 4) / 0
      }

      "return correct sparsity" in {
        val v1 = Sparse(Map(0 -> 1, 1 -> 2), 10)

        v1.sparsity() shouldEqual 0.8
      }

    }

  }

}
