package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging

class ShapeSuite extends FunSuite with Logging {
  test("ShapeSuite Basic Shape Test") {
    val rect = Shape((0.0, 0.0), 0.5, 1.0)
    assert(rect.toCoor.toString == "List((-1,0), (0,0), (1,0))")

    val rect1 = Shape((0.0, 0.0), (1.0, 1.0))
    assert(rect1.toCoor.toString == "List((0,0), (0,1), (1,0), (1,1))")

    val circle = Shape((0.0, 0.0), 2.0)
    assert(circle.toCoor.toString == 
      "List((-2,0), (-1,-1), (-1,0), (-1,1), (0,-2), (0,-1), (0,0), (0,1), (0,2), (1,-1), (1,0), (1,1), (2,0))")

    val ellipse = Shape((0.0, 0.0), 2.0, 1.0, 0.0)
    assert(ellipse.toCoor.toString == 
      "List((-2,0), (-1,0), (0,-1), (0,0), (0,1), (1,0), (2,0))")

    val list = List((0, 0), (0, 1), (1, 0), (1, 1))
    val shape = Shape(list)
    assert(shape.toCoor == list)
  }
}