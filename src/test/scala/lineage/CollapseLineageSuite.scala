package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.ImageMetadata
import workflow._

class CollapseLineageSuite extends FunSuite with LocalSparkContext with Logging {
  test("CollapseLineage Vector2Int Test"){
    sc = new SparkContext("local", "test")
    val v = DenseVector.zeros[Double](5)
    val l = List.fill(5){v}
    val inRDD = sc.parallelize(l)
    val il = List(1,2,3,4,5)
    val outRDD = sc.parallelize(il)
    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = CollapseLineage(inRDD, outRDD, transformer)
    assert(lineage.qForward(List(Coor(0,0))) == List(Coor(0,0)))
    assert(lineage.qBackward(List(Coor(0,0))) == List(Coor(0,0),Coor(0,1),Coor(0,2),Coor(0,3),Coor(0,4)))
  }

  test("CollapseLineage Matrix2Vector Test"){
    sc = new SparkContext("local", "test")
    val m = DenseMatrix.zeros[Double](2, 2)
    val l = List.fill(5){m}
    val inRDD = sc.parallelize(l)
    val v = DenseVector.zeros[Double](2)
    val vl = List.fill(5){v}
    val outRDD = sc.parallelize(vl)
    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = CollapseLineage(inRDD, outRDD, transformer,1)
    assert(lineage.qForward(List(Coor(0,0,0))) == List(Coor(0,0)))
    assert(lineage.qBackward(List(Coor(0,0))) == List(Coor(0,0,0), Coor(0,0,1)))
  }

  test("CollapseLineage Image2Matrix Test"){
    sc = new SparkContext("local", "test")
    val meta = ImageMetadata(2,2,3)
    val il = List.fill(5){meta}
    val inRDD = sc.parallelize(il)
    val m = DenseMatrix.zeros[Double](2, 2)
    val l = List.fill(5){m}
    val outRDD = sc.parallelize(l)
    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = CollapseLineage(inRDD, outRDD, transformer,2)
    assert(lineage.qForward(List(Coor(0,0,0,0))) == List(Coor(0,0,0)))
    assert(lineage.qBackward(List(Coor(0,0,0))) == List(Coor(0,0,0,0), Coor(0,0,0,1), Coor(0,0,0,2)))
  }
}