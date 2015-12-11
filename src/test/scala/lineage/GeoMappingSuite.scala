package lineage

import archery._
import org.scalatest.FunSuite
import pipelines.Logging

class GeoMappingSuite extends FunSuite with Logging {
  test("Geo Matrix-to-Matrix Mapping test"){
    val c1 = Shape((2.0, 2.0), 2.0)
    val c2 = Shape((2.0, 5.0), 2.0)
    val s1 = Shape((1.0, 2.0), 1.0, 1.0)
    val s2 = Shape((1.0, 4.0), 1.0, 1.0)
    val l = List((c1, s1), (c2, s2))
    val mapping = GeoMapping(l)
      
    val fResult = mapping.qForward(List(Coor(2,2)))
    assert(fResult.toString == "List((0,1), (0,2), (0,3), (1,1), (1,2), (1,3), (2,1), (2,2), (2,3))")

    val fResult1 = mapping.qForward(List(Coor(0,0)))
    println(fResult1.toString == "List()")

    val bResult = mapping.qBackward(List(Coor(1, 2)))
    assert(bResult.toString == "List((0,2), (1,1), (1,2), (1,3), (2,0), (2,1), (2,2), (2,3), (2,4), (3,1), (3,2), (3,3), (4,2))")
  }
}