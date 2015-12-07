package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class LinComMappingSuite extends FunSuite with Logging {
  test("LinComMapping Vector Test"){
    val v1 = DenseVector.zeros[Double](4)
    val v2 = DenseVector.zeros[Double](4)
    val mapping = LinComMapping(SubSpace(v1), SubSpace(v2))
    assert(mapping.qForward(List(Coor(0))).toString == "List(0, 1, 2, 3)")
    assert(mapping.qBackward(List(Coor(0))).toString == "List(0, 1, 2, 3)")
  }
}