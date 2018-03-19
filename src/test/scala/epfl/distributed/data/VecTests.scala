package epfl.distributed.data

import epfl.distributed.Spec

class VecTests extends Spec {

  "Vec" when {

    "being used" should {

      "work" in {

        val v1 = Vec(1, 2, 3)
        val v2 = Vec(1, 2, 3)

        v1 + v2 shouldBe Vec(2, 4, 6)
        v1 dot v2 shouldBe (1 + 4 + 9)
        v1 * 2 shouldBe Vec(1, 4, 9)
        v1.norm shouldBe Math.sqrt(1 + 4 + 9)

      }


    }

  }

}
