package lineage

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata
import workflow._

class TransposeLineageSuite extends FunSuite with Logging {
  test("TranposeLineage Vector Test"){
    val sc = new SparkContext("local", "test")
    val v1 = DenseVector.zeros[Double](4)

    val rdd = sc.parallelize(List.fill(2){v1})
    val input = Seq(rdd, rdd)

    val s = Seq(v1, v1)
    val output = sc.parallelize(List.fill(2){s})

    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = TransposeLineage(input, output, (0,1), transformer)
    assert(lineage.qForward(List(Coor(0,1,0))) == List(Coor(1,0,0)))
    assert(lineage.qBackward(List(Coor(1,0,0))) == List(Coor(0,1,0)))
  }
}