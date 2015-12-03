package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class IdentityMappingSuite extends FunSuite with Logging {
  test("IdentityMapping Vector Test"){
    val v1 = DenseVector.zeros[Double](5)
    val v2 = DenseVector.zeros[Double](5)
    val mapping = IdentityMapping(v1, v2)
    assert(mapping.qForward(List(Coor(0))).toString == "List(0)")

    intercept[java.lang.IllegalArgumentException] {
      mapping.qForward(List(Coor(5)))
    }
  }

  test("IdentityMapping Matrix Test"){
    val m1 = DenseMatrix.zeros[Double](5,5)
    val m2 = DenseMatrix.zeros[Double](5,5)
    val mapping = IdentityMapping(m1, m2)
    assert(mapping.qForward(List(Coor(0,0))).toString == "List((0,0))")

    intercept[java.lang.IllegalArgumentException] {
      mapping.qForward(List(Coor(5,5)))
    }
  }

  test("IdentityMapping Image Test"){
    val i1 = ImageMetadata(5,4,3)
    val i2 = ImageMetadata(5,4,3)
    val mapping = IdentityMapping(i1, i2)
    assert(mapping.qForward(List(Coor(0,0,0))).toString == "List((0,0,0))")

    intercept[java.lang.IllegalArgumentException] {
      mapping.qForward(List(Coor(5,4,3)))
    }
  }
}