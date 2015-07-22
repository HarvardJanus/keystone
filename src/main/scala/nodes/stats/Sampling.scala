package nodes.stats

import breeze.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.rdd.RDD
import pipelines.FunctionNode
import workflow._
import workflow.Lineage._
/**
 * Given a collection of Dense Matrices, this will generate a sample of `numSamples` columns from the entire set.
 * @param numSamples
 */
class ColumnSampler(
    numSamples: Int,
    numImgsOpt: Option[Int] = None)
  extends FunctionNode[RDD[DenseMatrix[Float]], RDD[DenseVector[Float]]] {

  def apply(in: RDD[DenseMatrix[Float]]): RDD[DenseVector[Float]] = {
    val numImgs = numImgsOpt.getOrElse(in.count.toInt)
    val samplesPerImage = numSamples/numImgs

    val outRDD = in.map(mat => {
      (0 until samplesPerImage).map( x => {
        val random = scala.util.Random.nextInt(mat.cols)
        (mat(::, random).toDenseVector, random)
      })
    })
    val squareListRDD = outRDD.map( x => {
      x.map( t => {
        val size = t._1.size
        val random = t._2
        Square((0, random), (size, random))
      }).toList
    })
    println("222222222222")
    val out = outRDD.flatMap(x => x.map(t=>t._1))
    val lineage = ShapeLineage(in, out, squareListRDD)
    println("333333333333")
    lineage.save("ColumnSampler-"+System.nanoTime())
    println("collecting lineage for ColumnSampler \t mapping: "+lineage.qBackward((0, 0)))
    out
  }

}

/**
 * Takes a sample of an input RDD of size size.
 * @param size Number of elements to return.
 */
class Sampler[T](val size: Int, val seed: Int = 42) extends FunctionNode[RDD[T], Array[T]] {
  def apply(in: RDD[T]): Array[T] = {
    in.takeSample(false, size, seed)
  }
}
