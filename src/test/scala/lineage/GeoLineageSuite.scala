package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.ImageMetadata
import workflow._

class GeoLineageSuite extends FunSuite with LocalSparkContext with Logging {
  test("GeoLineage Matrix2Vector Test"){
    sc = new SparkContext("local", "test")

    val m = DenseMatrix.zeros[Double](3, 2)
    val l = List.fill(5){m}
    val inRDD = sc.parallelize(l)
    val v = DenseVector.zeros[Double](6)
    val vl = List.fill(5){v}
    val outRDD = sc.parallelize(vl)

    val c1 = Shape((2.0, 2.0), 2.0)
    val c2 = Shape((2.0, 5.0), 2.0)
    val s1 = Shape((1.0, 2.0), 1.0, 1.0)
    val s2 = Shape((1.0, 4.0), 1.0, 1.0)
    val sl = List((c1, s1), (c2, s2))
    val ll = List.fill(5){sl}
    val llRDD = sc.parallelize(ll)
    val transformer = Transformer[Int, Int](_ * 1)

    val lineage = GeoLineage(inRDD, outRDD, llRDD, transformer)
    assert(lineage.qForward(List(Coor(0,2,2))) == 
      List(Coor(0,0,1), Coor(0,0,2), Coor(0,0,3), Coor(0,1,1), Coor(0,1,2), 
        Coor(0,1,3), Coor(0,2,1), Coor(0,2,2), Coor(0,2,3)))

    assert(lineage.qBackward(List(Coor(0,2,2))) ==
      List(Coor(0,0,2), Coor(0,1,1), Coor(0,1,2), Coor(0,1,3), Coor(0,2,0), 
        Coor(0,2,1), Coor(0,2,2), Coor(0,2,3), Coor(0,2,4), Coor(0,3,1), 
        Coor(0,3,2), Coor(0,3,3), Coor(0,4,2)))
  }
}