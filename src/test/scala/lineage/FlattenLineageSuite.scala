package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.ImageMetadata
import workflow._

class FlattenLineageSuite extends FunSuite with LocalSparkContext with Logging {
  test("FlattenLineage Matrix2Vector Test"){
    sc = new SparkContext("local", "test")
    val m = DenseMatrix.zeros[Double](3, 2)
    val l = List.fill(5){m}
    val inRDD = sc.parallelize(l)
    val v = DenseVector.zeros[Double](6)
    val vl = List.fill(5){v}
    val outRDD = sc.parallelize(vl)
    val transformer = Transformer[Int, Int](_ * 1)

    /*
     *  Breaking yDim
     */
    val lineage = FlattenLineage(inRDD, outRDD, transformer, 1)
    assert(lineage.qForward(List(Coor(0,0,1))) == List(Coor(0,3)))
    assert(lineage.qBackward(List(Coor(0,3))) == List(Coor(0,0,1)))

    /*
     *  Breaking xDim
     */
    val lineage2 = FlattenLineage(inRDD, outRDD, transformer, 0)
    assert(lineage2.qForward(List(Coor(0,1,1))) == List(Coor(0,3)))
    assert(lineage2.qBackward(List(Coor(0,3))) == List(Coor(0,1,1)))
  }

  test("FlattenLineage Vector2Matrix Test"){
    sc = new SparkContext("local", "test")
    val m = DenseMatrix.zeros[Double](3, 2)
    val l = List.fill(5){m}
    val outRDD = sc.parallelize(l)
    val v = DenseVector.zeros[Double](6)
    val vl = List.fill(5){v}
    val inRDD = sc.parallelize(vl)
    val transformer = Transformer[Int, Int](_ * 1)

    /*
     *  Breaking yDim
     */
    val lineage = FlattenLineage(inRDD, outRDD, transformer, 1)
    assert(lineage.qForward(List(Coor(0,3))) == List(Coor(0,0,1)))
    assert(lineage.qBackward(List(Coor(0,0,1))) == List(Coor(0,3)))

    /*
     *  Breaking xDim
     */
    val lineage2 = FlattenLineage(inRDD, outRDD, transformer, 0)
    assert(lineage2.qForward(List(Coor(0,3))) == List(Coor(0,1,1)))
    assert(lineage2.qBackward(List(Coor(0,1,1))) == List(Coor(0,3)))
  }

  test("FlattenLineage Seq[Vector]2Vector Test"){
    sc = new SparkContext("local", "test")
    val v = DenseVector.zeros[Double](3)
    val s = Seq(v, v)
    val sl = List.fill(5){s}
    val inRDD = sc.parallelize(sl)

    val v2 = DenseVector.zeros[Double](6)
    val v2l = List.fill(5){v2}
    val outRDD = sc.parallelize(v2l)

    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = FlattenLineage(inRDD, outRDD, transformer)
    assert(lineage.qForward(List(Coor(0,1,0))) == List(Coor(0,3)))
    assert(lineage.qBackward(List(Coor(0,3))) == List(Coor(0,1,0)))
  }
}