package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

abstract class Lineage extends serializable {
  val path = "Lineage"
  def qForward(keys: List[Coor]): List[Coor]
  def qBackward(keys: List[Coor]): List[Coor]
  def saveInput()
  def saveOutput()
  def saveMapping()
}

case class NarrowLineage(inRDD: RDD[_], outRDD: RDD[_], mappingRDD: RDD[_], transformer: Transformer[_,_]) extends Lineage{
  def qForward(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k:Coor3D => {
          val resultRDD = mappingRDD.zipWithIndex.map{
            case (mapping, index) => {
              if(index == k.x) mapping.asInstanceOf[Mapping].qForward(List(k.lower()))
            }
          }
          val filteredRDD = resultRDD.zipWithIndex.filter{
            case (result, index) => (index == k.x)
          }.map(_._1)
          val m = filteredRDD.first
          val innerRet = m.asInstanceOf[List[Coor]]
          innerRet.map(x => x.asInstanceOf[Coor].raise(k.x))
        }
      }
    })
  }

  def qBackward(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k:Coor3D => {
          val resultRDD = mappingRDD.zipWithIndex.map{
            case (mapping, index) => {
              if(index == k.x) mapping.asInstanceOf[Mapping].qBackward(List(k.lower()))
            }
          }
          val filteredRDD = resultRDD.zipWithIndex.filter{
            case (result, index) => (index == k.x)
          }.map(_._1)
          val m = filteredRDD.first
          val innerRet = m.asInstanceOf[List[Coor]]
          innerRet.map(x => x.asInstanceOf[Coor].raise(k.x))
        }
      }
    })
  }
  def saveInput() = {}
  def saveOutput() = {}
  def saveMapping() = {}
}

object Lineage{
  def apply(inRDD: RDD[_], outRDD: RDD[_], tupleListRDD: RDD[List[(Shape,Shape)]], transformer: Transformer[_,_]) = {
    val geoMappingRDD = tupleListRDD.map(l => GeoMapping(l))
    new NarrowLineage(inRDD, outRDD, geoMappingRDD, transformer)
  }
}

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