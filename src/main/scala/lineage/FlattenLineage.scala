package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

object FlattenLineage{
  def apply[T](inRDD: RDD[_], outRDD:RDD[_], transformer: Transformer[_, _], dim: Int=0) = {
    val mappingRDD = inRDD.zip(outRDD).map{
      case (in: DenseMatrix[_], out: DenseVector[_]) => {
        FlattenMapping(in, out, dim)
      }
      case (in: DenseVector[_], out: DenseMatrix[_]) => {
        FlattenMapping(in, out, dim)
      }
      case (in: Seq[DenseVector[T] @unchecked], out: DenseVector[T]) => {
        FlattenMapping(in, out)
      }
    }
    new NarrowLineage(inRDD, outRDD, mappingRDD, transformer)
  }
}