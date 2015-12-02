package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging

class MappingSuite extends FunSuite with Logging {
  test("IdentityMapping Test"){
    val s1 = Vector(5)
    val s2 = Vector(5)
    val mapping = IdentityMapping(s1, s2)
    assert(mapping.qForward(List(Coor(0))).toString == "List(0)")
    assert(mapping.qForward(List(Coor(5))).toString == "List(null)")
  }
}