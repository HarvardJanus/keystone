package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class SubSpaceSuite extends FunSuite with Logging {
  test("Vector SubSpace Test"){
    val v = DenseVector.zeros[Double](5)
    val vm = SubSpace(v.size)
    assert(vm.toString == "Vector: 5")
  }

  test("Matrix SubSpace Test"){
    val m = DenseMatrix.zeros[Double](5,5)
    val mm = SubSpace(m.rows, m.cols)
    assert(mm.toString == "Matrix: 5x5")
  }

  test("Image SubSpace Test"){
    val meta = ImageMetadata(5, 4, 3)
    val nativeMeta = SubSpace(meta)
    assert(nativeMeta.toString == "Image: 5x4x3")

    val metadata = SubSpace(5, 4, 3)
    assert(metadata.toString == "Image: 5x4x3")
  }
}