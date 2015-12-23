package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage, ImageMetadata}
import workflow._

object CollapseLineage{
  def apply(inRDD: RDD[_], outRDD:RDD[_], transformer: Transformer[_, _], dim: Int=0) = {
    val mappingRDD = inRDD.zip(outRDD).map{
      case (in: DenseVector[_], out: Int) => {
        CollapseMapping(in, out)
      }
      case (in: DenseMatrix[_], out: DenseVector[_]) => {
        CollapseMapping(in, out, dim)
      }
      case (in: KeystoneImage, out: DenseMatrix[_]) => {
        CollapseMapping(in, out, dim)
      }
      case (in: ImageMetadata, out: DenseMatrix[_]) => {
        CollapseMapping(in, out, dim)
      }
    }
    new NarrowLineage(inRDD, outRDD, mappingRDD, transformer)
  }
}