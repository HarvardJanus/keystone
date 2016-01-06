package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class LinComMappingSuite extends FunSuite with Logging {
  test("LinComMapping Vector Test"){
    val v1 = DenseVector.zeros[Double](4)
    val v2 = DenseVector.zeros[Double](4)
    val mapping = LinComMapping(v1, v2)
    assert(mapping.qForward(List(Coor(0))).toString == "List(0, 1, 2, 3)")
    assert(mapping.qBackward(List(Coor(0))).toString == "List(0, 1, 2, 3)")
  }

  test("LinComMapping Matrix Test"){
    val m1 = DenseMatrix.zeros[Double](5, 4)
    val m2 = DenseMatrix.zeros[Double](5, 2)
    val mapping = LinComMapping(m1, m2)
    assert(mapping.qForward(List(Coor(2,0))) == List(Coor(2,0), Coor(2,1)))
    assert(mapping.qBackward(List(Coor(2,0))) == List(Coor(2,0), Coor(2,1), Coor(2,2), Coor(2,3)))
  }
}