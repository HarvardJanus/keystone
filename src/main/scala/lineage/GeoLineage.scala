package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

object GeoLineage{
  def apply(inRDD: RDD[_], outRDD: RDD[_], tupleListRDD: RDD[List[(Shape,Shape)]], transformer: Transformer[_,_]) = {
    val geoMappingRDD = tupleListRDD.map(l => GeoMapping(l))
    new NarrowLineage(inRDD, outRDD, geoMappingRDD, transformer)
  }
}