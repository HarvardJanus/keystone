package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

abstract class Lineage extends serializable {
  //val path = "Lineage"
  def qForward(keys: List[Coor]): List[Coor]
  def qBackward(keys: List[Coor]): List[Coor]
  def saveInput()
  def saveOutput(tag: String)
  def saveOutputSmart(tag: String, duration: Long)
  def saveMapping(tag: String)
}

object Lineage{
  val path = "Lineage"
  val pathTrial = "Lineage/Trial"
  var stamp = System.nanoTime()  
  def updateStamp(nStamp: Long) = {
    stamp = nStamp
  }
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
  def saveOutput(tag: String) = {
    outRDD.saveAsObjectFile(Lineage.path+"/"+tag+"/outRDD")
    //Lineage.updateStamp(System.nanoTime())
    //println(Lineage.stamp)
  }

  def saveOutputSmart(tag: String, duration: Long) = {
    val numTrials = 3
    val trialTimeList = (0 until numTrials).map(i => {
      val sampleRDD = outRDD.sample(true, 0.1)
      val path = Lineage.pathTrial+"/"+tag+"/outRDD-"+i
      sampleRDD.saveAsObjectFile(path)
      val sc = sampleRDD.context
      sampleRDD.unpersist()
      val rdd = sc.objectFile(path)
      time(rdd.count)
    }).toList
    val predictedLoadTime = trialTimeList.sum*10/trialTimeList.length

    val outPath = Lineage.path+"/"+tag+"/outRDD"
    outRDD.saveAsObjectFile(outPath)
    outRDD.unpersist()
    val sc = outRDD.context
    val rdd = sc.objectFile(outPath)
    val loadTime = time(rdd.count)
    
    println(tag+" predictedLoadTime: "+predictedLoadTime+" actualLoadTime: "+loadTime)  
  }

  def saveMapping(tag: String) = {
    mappingRDD.saveAsObjectFile(Lineage.path+"/"+tag+"/mappingRDD")
  }

  /*
   *  Helper function to record time
   */
  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    System.nanoTime-s/1e9
  }
}

case class TransposeLineage[T](inRDD: Seq[RDD[DenseVector[T]]], outRDD: RDD[Seq[DenseVector[T]]], mapping: JoinMapping, transformer: Transformer[_,_]) extends Lineage{
  def qForward(keys: List[Coor]) = mapping.qForward(keys)
  def qBackward(keys: List[Coor]) = mapping.qBackward(keys)
  def saveInput() = {}
  def saveOutput(tag: String) = {}
  def saveOutputSmart(tag: String, duration: Long) = {}
  def saveMapping(tag: String) = {}
}