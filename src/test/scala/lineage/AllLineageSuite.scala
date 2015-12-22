package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import workflow.Transformer

class AllLineageSuite extends FunSuite with LocalSparkContext with Logging {
  test("AllLineage Vector Test"){
    sc = new SparkContext("local", "test")
    val v = DenseVector.zeros[Double](5)
    val l = List.fill(5){v}
    val inRDD = sc.parallelize(l)
    val outRDD = sc.parallelize(l)
    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = AllLineage(inRDD, outRDD, transformer)
    assert(lineage.qForward(List(Coor(0,0))) == List(Coor(0,0),Coor(0,1),Coor(0,2),Coor(0,3),Coor(0,4)))
    assert(lineage.qBackward(List(Coor(0,4))) == List(Coor(0,0),Coor(0,1),Coor(0,2),Coor(0,3),Coor(0,4)))
  }

  test("AllLineage Matrix Test"){
    sc = new SparkContext("local", "test")
    val v = DenseMatrix.zeros[Double](2, 2)
    val l = List.fill(5){v}
    val inRDD = sc.parallelize(l)
    val outRDD = sc.parallelize(l)
    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = AllLineage(inRDD, outRDD, transformer)
    assert(lineage.qForward(List(Coor(0,1,1))) == List(Coor(0,0,0), Coor(0,0,1), Coor(0,1,0), Coor(0,1,1)))
    assert(lineage.qBackward(List(Coor(0,0,0))) == List(Coor(0,0,0), Coor(0,0,1), Coor(0,1,0), Coor(0,1,1)))
  }
}