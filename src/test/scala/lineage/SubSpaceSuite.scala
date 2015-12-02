package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class SubSpaceSuite extends FunSuite with Logging {
  test("Vector SubSpace Test"){
    val v = DenseVector.zeros[Double](5)
    val m = SubSpace(v)
    assert(m.toString == "Vector: 5")
  }

  test("Vector SubSpace Contain Test"){
    val v = DenseVector.zeros[Double](5)
    val m = SubSpace(v)
    assert(m.contain(Coor(0)) == true)
    assert(m.contain(Coor(5)) == false)

    intercept[java.lang.IllegalArgumentException] {
      m.contain(Coor(0,0))
    }
  }

  test("Vector SubSpace Expand Test"){
    val v = DenseVector.zeros[Double](5)
    val m = SubSpace(v)
    assert(m.expand.toString == "List(0, 1, 2, 3, 4)")
  }

  test("Matrix SubSpace Test"){
    val m = DenseMatrix.zeros[Double](5,5)
    val s = SubSpace(m)
    assert(s.toString == "Matrix: 5x5")
  }

  test("Matrix SubSpace Contain Test"){
    val v = DenseMatrix.zeros[Double](5,5)
    val m = SubSpace(v)
    assert(m.contain(Coor(0,0)) == true)
    assert(m.contain(Coor(5,0)) == false)
  }

  test("Matrix SubSpace Expand Test"){
    val v = DenseMatrix.zeros[Double](3,2)
    val m = SubSpace(v)
    assert(m.expand.toString == "List((0,0), (0,1), (1,0), (1,1), (2,0), (2,1))")
  }

  test("Image SubSpace Test"){
    val meta = ImageMetadata(5, 4, 3)
    val nativeMeta = SubSpace(meta)
    assert(nativeMeta.toString == "Image: 5x4x3")

    val metadata = SubSpace(5, 4, 3)
    assert(metadata.toString == "Image: 5x4x3")
  }

  test("Image SubSpace Contain Test"){
    val meta = ImageMetadata(5, 4, 3)
    val s = SubSpace(meta)
    assert(s.contain(Coor(0,0,0)) == true)
    assert(s.contain(Coor(0,0,3)) == false)
  }

  test("Image SubSpace Expand Test"){
    val meta = ImageMetadata(3, 2, 1)
    val s = SubSpace(meta)
    assert(s.expand.toString == "List((0,0,0), (0,1,0), (1,0,0), (1,1,0), (2,0,0), (2,1,0))")
  }

  test("SubSpace Contain CoorNull Test"){
    val v = DenseVector.zeros[Double](5)
    val vs = SubSpace(v)
    intercept[java.lang.IllegalArgumentException] {
      vs.contain(Coor())
    }

    val m = DenseMatrix.zeros[Double](5,5)
    val ms = SubSpace(m)
    intercept[java.lang.IllegalArgumentException] {
      vs.contain(Coor())
    }

    val meta = ImageMetadata(5,4,3)
    val is = SubSpace(meta)
    intercept[java.lang.IllegalArgumentException] {
      is.contain(Coor())
    }
  }
}