package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

object AllLineage{
  def apply(inRDD: RDD[_], outRDD:RDD[_], transformer: Transformer[_, _]) = {
    val mappingRDD = inRDD.zip(outRDD).map{
      case (in: DenseVector[_], out: DenseVector[_]) => {
        AllMapping(in, out)
      }
      case (in: DenseMatrix[_], out: DenseMatrix[_]) => {
        AllMapping(in, out)
      }
      case (in: KeystoneImage, out: KeystoneImage) => {
        AllMapping(in, out)
      }
    }
    new NarrowLineage(inRDD, outRDD, mappingRDD, transformer)
  }
}