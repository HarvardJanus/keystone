package lineage

import org.scalatest.FunSuite
import pipelines.Logging

class CoordinateSuite extends FunSuite with Logging {
  test("1D Coordinate Test"){
    val c = Coor(5)
    assert(c.toString == "5")
  }

  test("2D Coordinate Test"){
    val c = Coor(2,5)
    assert(c.toString == "(2, 5)")
  }

  test("3D Coordinate Test"){
    val c = Coor(2,3,5)
    assert(c.toString == "(2, 3, 5)")
  }
}