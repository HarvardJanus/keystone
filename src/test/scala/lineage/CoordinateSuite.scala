package lineage

import org.scalatest.FunSuite
import pipelines.Logging

class CoordinateSuite extends FunSuite with Logging {
  test("1D Coordinate Test"){
    val c = Coor(5)
    assert(c.toString == "5")
  }

  test("1D Negative Coordinate Test"){
    intercept[java.lang.IllegalArgumentException] {
      val cc = Coor(-1)
    }
  }

  test("2D Coordinate Test"){
    val c = Coor(2,5)
    assert(c.toString == "(2, 5)")
  }

  test("2D Negative Coordinate Test"){
    intercept[java.lang.IllegalArgumentException] {
      val cc = Coor(-1, 0)
    }
  }

  test("3D Coordinate Test"){
    val c = Coor(2,3,5)
    assert(c.toString == "(2, 3, 5)")
  }

  test("3D Negative Coordinate Test"){
    intercept[java.lang.IllegalArgumentException] {
      val cc = Coor(-1, 0, 0)
    }    
  }
}