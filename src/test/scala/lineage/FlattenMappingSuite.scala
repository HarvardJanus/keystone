package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class FlattenMappingSuite extends FunSuite with Logging {
  test("FlattenMapping Matrix2Vector Test"){
    val m = DenseMatrix.zeros[Double](5, 4)
    val v = DenseVector.zeros[Double](20)

    /*
     *  Flatten a matrix by breaking the yDim
     */
    val mapping1 = FlattenMapping(m, v, 1)
    assert(mapping1.qForward(List(Coor(3,2))).toString == "List(13)")
    assert(mapping1.qBackward(List(Coor(13))).toString == "List((3,2))")

    /*
     *  Flatten a matrix by breaking the xDim
     */
    val v2 = DenseVector.zeros[Double](5)
    val mapping2 = FlattenMapping(m, v, 0)
    assert(mapping2.qForward(List(Coor(3,2))).toString == "List(14)")
    assert(mapping2.qBackward(List(Coor(14))).toString == "List((3,2))")
  }

  test("FlattenMapping Seq[Vector]2Vector Test"){
    val v1 = DenseVector.zeros[Double](5)
    val s = Seq(v1, v1)
    val v2 = DenseVector.zeros[Double](10)

    val mapping = FlattenMapping(s, v2)
    assert(mapping.qForward(List(Coor(1,0))).toString == "List(5)")
    assert(mapping.qBackward(List(Coor(5))).toString == "List((1,0))")
  }

  test("FlattenMapping Vector2Matrix Test"){
    val m = DenseMatrix.zeros[Double](5, 4)
    val v = DenseVector.zeros[Double](20)

    /*
     *  Flatten a matrix by breaking the yDim
     */
    val mapping1 = FlattenMapping(v, m, 1)
    assert(mapping1.qForward(List(Coor(13))).toString == "List((3,2))")
    assert(mapping1.qBackward(List(Coor(3,2))).toString == "List(13)")

    /*
     *  Flatten a matrix by breaking the xDim
     */
    val v2 = DenseVector.zeros[Double](5)
    val mapping2 = FlattenMapping(v, m, 0)
    assert(mapping2.qForward(List(Coor(14))).toString == "List((3,2))")
    assert(mapping2.qBackward(List(Coor(3,2))).toString == "List(14)")
  }
}