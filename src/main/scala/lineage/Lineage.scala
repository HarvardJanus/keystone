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
  def saveMapping(tag: String)
}

case class NarrowLineage(inRDD: RDD[_], outRDD: RDD[_], mappingRDD: RDD[_], transformer: Transformer[_,_], model: DenseMatrix[_]=null) extends Lineage{
  def qForward(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k:Coor => {
          val resultRDD = mappingRDD.zipWithIndex.map{
            case (mapping, index) => {
              if(index == k.first) mapping.asInstanceOf[Mapping].qForward(List(k.lower()))
            }
          }
          val filteredRDD = resultRDD.zipWithIndex.filter{
            case (result, index) => (index == k.first)
          }.map(_._1)
          val m = filteredRDD.first
          val innerRet = m.asInstanceOf[List[Coor]]
          innerRet.map(x => x.asInstanceOf[Coor].raise(k.first))
        }
      }
    })
  }

  def qBackward(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k:Coor => {
          val resultRDD = mappingRDD.zipWithIndex.map{
            case (mapping, index) => {
              if(index == k.first) mapping.asInstanceOf[Mapping].qBackward(List(k.lower()))
            }
          }
          val filteredRDD = resultRDD.zipWithIndex.filter{
            case (result, index) => (index == k.first)
          }.map(_._1)
          val m = filteredRDD.first
          val innerRet = m.asInstanceOf[List[Coor]]
          innerRet.map(x => x.asInstanceOf[Coor].raise(k.first))
        }
      }
    })
  }
  def saveInput() = {}
  def saveOutput() = {}
  def saveMapping(tag: String) = mappingRDD.saveAsObjectFile(path+"/"+tag+"/mappingRDD")
}

case class TransposeLineage[T](inRDD: Seq[RDD[DenseVector[T]]], outRDD: RDD[Seq[DenseVector[T]]], mapping: JoinMapping, transformer: Transformer[_,_]) extends Lineage{
  def qForward(keys: List[Coor]) = mapping.qForward(keys)
  def qBackward(keys: List[Coor]) = mapping.qBackward(keys)
  def saveInput() = {}
  def saveOutput() = {}
  def saveMapping(tag: String) = {}
}