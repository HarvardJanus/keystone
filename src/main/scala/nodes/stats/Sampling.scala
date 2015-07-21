package nodes.stats

import breeze.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.rdd.RDD
import pipelines.FunctionNode

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

    val out = in.flatMap(mat => {
      (0 until samplesPerImage).map( x => {
        mat(::, scala.util.Random.nextInt(mat.cols)).toDenseVector
      })
    })
    val m = in.take(1)(0)
    val v = out.take(1)(0)
    println("numImgs: "+numImgs)
    println("samplesPerImage: "+samplesPerImage)
    println("matrix dimension: "+m.rows+"x"+m.cols)
    println("vector dimension: "+v.size)
    println("total num of vectors: "+out.count)
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
