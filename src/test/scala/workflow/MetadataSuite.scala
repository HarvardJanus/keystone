package workflow

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.Logging
import utils.{ImageUtils, TestUtils}
import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

class MetadataSuite extends FunSuite with Logging {
  test("Vector Metadata Test"){
    val v = DenseVector.zeros[Double](5)
    val vm = Metadata(v.size)
    println(vm)
    assert(vm.toString == "Vector: 5")
  }

  test("Matrix Metadata Test"){
    val m = DenseMatrix.zeros[Double](5,5)
    val mm = Metadata(m.rows, m.cols)
    println(mm)
    assert(mm.toString == "Matrix: 5x5")
  }

  test("Image Metadata Test"){
    val meta = ImageMetadata(5, 5, 3)
    val nativeMeta = Metadata(meta)
    println(nativeMeta)
    assert(nativeMeta.toString == "Image: 5x5x3")

    val metadata = Metadata(5, 5, 3)
    assert(metadata.toString == "Image: 5x5x3")
  }
}