package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class AllMappingSuite extends FunSuite with Logging {
  test("AllMapping Vector Test"){
    val v1 = DenseVector.zeros[Double](5)
    val v2 = DenseVector.zeros[Double](5)
    val mapping = AllMapping(v1, v2)
    assert(mapping.qForward(List(Coor(0))).toString == 
      "List(0, 1, 2, 3, 4)")

    intercept[java.lang.IllegalArgumentException] {
      mapping.qForward(List(Coor(5)))
    }
  }

  test("AllMapping Matrix Test"){
    val m1 = DenseMatrix.zeros[Double](3,2)
    val m2 = DenseMatrix.zeros[Double](3,2)
    val mapping = AllMapping(m1, m2)
    assert(mapping.qForward(List(Coor(0,0))).toString == 
      "List((0,0), (0,1), (1,0), (1,1), (2,0), (2,1))")

    intercept[java.lang.IllegalArgumentException] {
      mapping.qForward(List(Coor(5,5)))
    }
  }

  test("AllMapping Image Test"){
    val i1 = ImageMetadata(3,2,1)
    val i2 = ImageMetadata(3,2,1)
    val mapping = AllMapping(i1, i2)
    assert(mapping.qForward(List(Coor(0,0,0))).toString == "List((0,0,0), (0,1,0), (1,0,0), (1,1,0), (2,0,0), (2,1,0))")

    intercept[java.lang.IllegalArgumentException] {
      mapping.qForward(List(Coor(3,2,1)))
    }
  }
}