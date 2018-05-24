package epfl.distributed.utils

import epfl.distributed.Spec

class DatasetTests extends Spec {

  "Dataset" when {

    "being load" should {

      "load fast enough" in {
        val chunk = 4096
        val folder = "data/"

        val start = System.currentTimeMillis()

        val data = Dataset.rcv1(folder, full = true, chunk)
        data.length shouldBe 804414

        val end = System.currentTimeMillis()

        val seconds = (end - start ) / 1000.0
        seconds should be < 40.0
      }

    }

  }

}
