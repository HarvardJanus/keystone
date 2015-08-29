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

    val outRDD = in.zipWithIndex.map{
      case (mat, index) => {
        (0 until samplesPerImage).map( x => {
          val random = scala.util.Random.nextInt(mat.cols)
          (mat(::, random).toDenseVector, index, random)
        })
      }
    }
    //cache to avoid re-evaluation
    //outRDD.cache()

    val out = outRDD.flatMap(x => x.map(t => t._1))
    out.cache()
    
    val bMappingRDD = outRDD.flatMap(x => x.map(t => (t._2, t._3.toLong)))


    val fTempRDD = outRDD.map(x => x.map(t => (t._2, t._3.toLong)))
    val fMappingRDD = fTempRDD.map( x => {
      x.toList.zipWithIndex.map{
        case ((index, random), innerIndex) => (random, index*samplesPerImage+innerIndex)
      }
    })

    val lineage = SampleLineage(in, out, fMappingRDD, bMappingRDD)
    lineage.save("ColumnSampler-"+System.nanoTime())
    println("collecting lineage for ColumnSampler")
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
