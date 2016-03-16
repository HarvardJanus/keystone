package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

object IdentityLineage{
  def apply(inRDD: RDD[_], outRDD:RDD[_], transformer: Transformer[_, _]) = {
    val mappingRDD = inRDD.zip(outRDD).map{
      case (in: DenseVector[_], out: DenseVector[_]) => {
        IdentityMapping(in, out)
      }
      case (in: DenseMatrix[_], out: DenseMatrix[_]) => {
        IdentityMapping(in, out)
      }
      case (in: KeystoneImage, out: KeystoneImage) => {
        IdentityMapping(in, out)
      }
    }
    new NarrowLineage(inRDD, outRDD, mappingRDD, transformer)
  }
}