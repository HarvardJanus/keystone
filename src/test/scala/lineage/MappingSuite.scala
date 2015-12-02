package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging

class MappingSuite extends FunSuite with Logging {
  test("IdentityMapping Test"){
    val v1 = DenseVector.zeros[Double](5)
    val v2 = DenseVector.zeros[Double](5)
    val mapping = IdentityMapping(v1, v2)
    assert(mapping.qForward(List(Coor(0))).toString == "List(0)")
    assert(mapping.qForward(List(Coor(5))).toString == "List(null)")
  }
}