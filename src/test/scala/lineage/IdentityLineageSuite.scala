package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import workflow.Transformer

class IdentityLineageSuite extends FunSuite with LocalSparkContext with Logging {
  test("IdentityLineage Vector Test"){
    sc = new SparkContext("local", "test")
    val v = DenseVector.zeros[Double](5)
    val l = List.fill(5){v}
    val inRDD = sc.parallelize(l)
    val outRDD = sc.parallelize(l)
    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = IdentityLineage(inRDD, outRDD, transformer)
    assert(lineage.qForward(List(Coor(0,0))) == List(Coor(0,0)))
    assert(lineage.qBackward(List(Coor(0,4))) == List(Coor(0,4)))
  }
}