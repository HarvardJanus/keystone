package workflow

import archery._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import scala.math._
import utils.{ImageUtils, TestUtils}


class ShapeSuite extends FunSuite with Logging {
  test("basic shape test") {
    val circle = Circle((0, 0), 2.0)
    assert(circle.toCoor == List((-2,0), (-1,-1), (-1,0), (-1,1), (0,-2), (0,-1), (0,0), (0,1), (0,2), (1,-1), (1,0), (1,1), (2,0)))

    val ellipse = Ellipse((0, 0), 2, 1, 0)
    assert(ellipse.toCoor == List((-2,0), (-1,0), (0,-1), (0,0), (0,1), (1,0), (2,0)))

    val square = Square((0, 0), 1, 1)
    assert(square.toCoor == List((-1,-1), (-1,0), (-1,1), (0,-1), (0,0), (0,1), (1,-1), (1,0), (1,1)))

    val square1 = Square((-1, -1), (1, 1))
    assert(square1.toCoor == List((-1,-1), (-1,0), (-1,1), (0,-1), (0,0), (0,1), (1,-1), (1,0), (1,1)))
  }

  test("shape detection test") {
  	val clist = List((-2,0), (-1,-1), (-1,0), (-1,1), (0,-2), (0,-1), (0,0), (0,1), (0,2), (1,-1), (1,0), (1,1), (2,0))
    val circle = Shape(clist)
    val objectCircle = new Circle((0.0,0.0), 2.0)
    assert(circle.toCoor == objectCircle.toCoor)

    val elist = List((-2,0), (-1,0), (0,-1), (0,0), (0,1), (1,0), (2,0))
    val ellipse = Shape(elist)
    val objectEllipse = new Ellipse((0.0,0.0), 2.0, 1.0, 0.0)
    assert(ellipse.toCoor == objectEllipse.toCoor)

    val slist = List((-1,-1), (-1,0), (-1,1), (0,-1), (0,0), (0,1), (1,-1), (1,0), (1,1))
    val square = Shape(slist)
    val objectSquare = new Square((0,0), 1, 1)
    assert(square.toCoor == objectSquare.toCoor)

    val rlist = List((0,0), (1,0), (2,0), (3,0), (4,0), (5,0), (6,0), (7,0))
    val shape = Shape(rlist)
    println(shape)
  }

  test("bounding box test") {
  	val circle = new Circle((0,0), 2)
  	assert(circle.toBox == Box(-2.0F, -2.0F, 2.0F, 2.0F))

  	val ellipse = new Ellipse((0,0), 2, 2, 0)
  	assert(ellipse.toBox == Box(-2.0F, -2.0F, 2.0F, 2.0F))

  	val ellipse2 = new Ellipse((0,0), 2, 1, Pi/4)
  	assert(ellipse2.toBox == Box(-1.5811388F,-1.5811388F,1.5811388F,1.5811388F))

  	val square = new Square((0,0), 2, 1)
  	assert(square.toBox == Box(-1.0F, -2.0F, 1.0F, 2.0F))
  }
}