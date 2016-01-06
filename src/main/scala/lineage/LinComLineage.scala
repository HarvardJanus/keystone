package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

object LinComLineage{
  def apply(inRDD: RDD[_], outRDD:RDD[_], transformer: Transformer[_, _], model: DenseMatrix[_]) = {
    val mappingRDD = inRDD.zip(outRDD).map{
      case (in: DenseVector[_], out: DenseVector[_]) => {
        LinComMapping(in, out)
      }
      case (in: DenseMatrix[_], out: DenseMatrix[_]) => {
        LinComMapping(in, out)
      }
    }
    new NarrowLineage(inRDD, outRDD, mappingRDD, transformer, model)
  }
}