package workflow

import breeze.linalg._
import breeze.stats.distributions._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import workflow._
import workflow.Lineage._

class LineageSuite extends FunSuite with LocalSparkContext with Logging {
  test("Batch Query Vector Test") {
    sc = new SparkContext("local", "test")

    val v = DenseVector.zeros[Double](5)
    val sRDD = sc.parallelize(List.fill(4){v})
    
    val addition = new AdditionNode()
    val tRDD = addition(sRDD)
    
    val lineage = AllToOneLineage(sRDD, tRDD, addition)

    val keyList = List((0,0), (0,1))
    val results = lineage.qBackwardBatch(keyList)
    assert(results == List((0,0), (0,1), (0,2), (0,3), (0,4), (0,0), (0,1), (0,2), (0,3), (0,4)))
  }

  test("Batch Query Matrix Test") {
    sc = new SparkContext("local", "test")

    val m = DenseMatrix.zeros[Double](5,5)
    val sRDD = sc.parallelize(List.fill(4){m})
    
    val addition = new MatrixAdditionNode()
    val tRDD = addition(sRDD)
    
    val lineage = AllToOneLineage(sRDD, tRDD, addition)

    val keyList = List((0,0,0))
    val results = lineage.qBackwardBatch(keyList)
    println(results)
    assert(results == List((0,0,0), (0,0,1), (0,0,2), (0,0,3), 
      (0,0,4), (0,1,0), (0,1,1), (0,1,2), (0,1,3), (0,1,4), (0,2,0), (0,2,1), 
      (0,2,2), (0,2,3), (0,2,4), (0,3,0), (0,3,1), (0,3,2), (0,3,3), (0,3,4), 
      (0,4,0), (0,4,1), (0,4,2), (0,4,3), (0,4,4)))
  }

  /*test("OneToOne Vector Lineage") {
  	sc = new SparkContext("local", "test")

  	val v = DenseVector.zeros[Double](5)
  	val sRDD = sc.parallelize(List.fill(4){v})
  	
  	val addition = new AdditionNode()
  	val tRDD = addition(sRDD)
  	
  	val lineage = OneToOneLineage(sRDD, tRDD, addition)

  	/*query forward*/
  	assert(lineage.qForward((0,2)) == List((0, 2)))
  	/*query backward*/
  	assert(lineage.qBackward((0,2)) == List((0, 2)))
  	/*query forward out of bound of RDD*/
  	intercept[java.lang.UnsupportedOperationException] {
  		println(lineage.qForward((4,2)))
  	}
  	/*query forward out of bound of a vector*/
  	intercept[org.apache.spark.SparkException] {
  		println(lineage.qForward((0,5)))
  	}
  	/*query forward with wrong dimensional key*/
  	intercept[org.apache.spark.SparkException] {
  		println(lineage.qForward((0,0,0)))
  	}
  }

  test("OneToOne Matrix Lineage") {
  	sc = new SparkContext("local", "test")
  	val m = DenseMatrix.zeros[Double](2, 2)
  	val sRDD = sc.parallelize(List.fill(4){m})

  	val addition = new MatrixAdditionNode()
  	val tRDD = addition(sRDD)
  	val lineage = OneToOneLineage(sRDD, tRDD, addition)

  	/*query forward*/
  	assert(lineage.qForward((0,1,1)) == List((0,1,1)))
  	/*query backward*/
  	assert(lineage.qBackward((0,1,1)) == List((0,1,1)))
  }*/
}

case class AdditionNode()
  extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = in :+= 2.0
}

case class MatrixAdditionNode()
  extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {
  def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = in :+= 2.0
}