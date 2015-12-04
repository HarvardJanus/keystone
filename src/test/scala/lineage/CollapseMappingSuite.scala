package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class CollapseMappingSuite extends FunSuite with Logging {
  test("CollapseMapping Matrix2Vector Test"){
    val m = DenseMatrix.zeros[Double](5, 4)
    val v = DenseVector.zeros[Double](4)
    val mapping1 = CollapseMapping(m, v, 0)
    assert(mapping1.qForward(List(Coor(0,0), Coor(0,1), Coor(1,1))).toString == "List(0, 1)")
    assert(mapping1.qBackward(List(Coor(0))).toString == "List((0,0), (1,0), (2,0), (3,0), (4,0))")

    val v2 = DenseVector.zeros[Double](5)
    val mapping2 = CollapseMapping(m, v, 1)
    assert(mapping2.qForward(List(Coor(0,0), Coor(0,1), Coor(1,1))).toString == "List(0, 1)")
    assert(mapping2.qBackward(List(Coor(0))).toString == "List((0,0), (0,1), (0,2), (0,3))")
  }

  test("CollapseMapping Image2Matrix Test"){
    val image = ImageMetadata(5,4,3)
    val m1 = DenseMatrix.zeros[Double](5,4)
    val mapping1 = CollapseMapping(image, m1, 2)
    assert(mapping1.qForward(List(Coor(0,0,0))).toString == "List((0,0))")
    assert(mapping1.qBackward(List(Coor(0,0))).toString == "List((0,0,0), (0,0,1), (0,0,2))")

    val m2 = DenseMatrix.zeros[Double](5,3)
    val mapping2 = CollapseMapping(image, m2, 1)
    assert(mapping2.qForward(List(Coor(0,0,0))).toString == "List((0,0))")
    assert(mapping2.qBackward(List(Coor(0,0))).toString == "List((0,0,0), (0,1,0), (0,2,0), (0,3,0))")

    val m3 = DenseMatrix.zeros[Double](4,3)
    val mapping3 = CollapseMapping(image, m3, 0)
    assert(mapping3.qForward(List(Coor(0,0,0))).toString == "List((0,0))")
    assert(mapping3.qBackward(List(Coor(0,0))).toString == "List((0,0,0), (1,0,0), (2,0,0), (3,0,0), (4,0,0))")
  }
}