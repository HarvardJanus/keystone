package lineage

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class JoinMappingSuite extends FunSuite with Logging {
  test("JoinMapping Vector Test"){
    val sc = new SparkContext("local", "test")
    val v1 = DenseVector.zeros[Double](4)
    val v2 = DenseVector.zeros[Double](4)

    val rdd = sc.parallelize(List.fill(2){v1})
    val input = Seq(rdd, rdd)

    val s = Seq(v1, v1)
    val output = sc.parallelize(List.fill(2){s})

    val mapping = JoinMapping(input, output)
    assert(mapping.qForward(List(Coor(0,1,0), Coor(1,0,2))).toString == "List((1,0,0), (0,1,2))")
    assert(mapping.qBackward(List(Coor(1,0,0), Coor(0,1,2))).toString == "List((0,1,0), (1,0,2))")
  }
}