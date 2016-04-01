package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import workflow.Transformer

class LinComLineageSuite extends FunSuite with LocalSparkContext with Logging {
  test("LinComLineage Vector Test"){
    sc = new SparkContext("local", "test")
    val v = DenseVector.zeros[Double](5)
    val l = List.fill(5){v}
    val inRDD = sc.parallelize(l)
    val outRDD = sc.parallelize(l)
    val transformer = Transformer[Int, Int](_ * 1)
    val model = DenseMatrix.zeros[Double](2, 2)

    val lineage = LinComLineage(inRDD, outRDD, transformer, model)
    assert(lineage.qForward(List(Coor(0,2))) == List(Coor(0,0),Coor(0,1),Coor(0,2),Coor(0,3),Coor(0,4)))
    assert(lineage.qBackward(List(Coor(0,4))) == List(Coor(0,0),Coor(0,1),Coor(0,2),Coor(0,3),Coor(0,4)))
  }

  test("LinComLineage Matrix Test"){
    sc = new SparkContext("local", "test")
    val v = DenseMatrix.zeros[Double](3, 4)
    val l = List.fill(5){v}
    val inRDD = sc.parallelize(l)

    val v2 = DenseMatrix.zeros[Double](2, 3)
    val l2 = List.fill(5){v2}
    val outRDD = sc.parallelize(l2)

    val transformer = Transformer[Int, Int](_ * 1)
    val model = DenseMatrix.zeros[Double](4, 2)

    val lineage = LinComLineage(inRDD, outRDD, transformer, model)
    assert(lineage.qForward(List(Coor(0,1,1))) == List(Coor(0,0,1), Coor(0,1,1)))
    assert(lineage.qBackward(List(Coor(0,1,1))) == List(Coor(0,0,1), Coor(0,1,1), Coor(0,2,1)))
  }
}