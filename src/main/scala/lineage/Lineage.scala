package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.annotation.tailrec
import sys.process.stringSeqToProcess
import utils.{Image=>KeystoneImage}
import workflow._

trait Queriable extends serializable{
  def qForward(keys: List[Coor]): List[Coor]
  def qBackward(keys: List[Coor]): List[Coor]
}

abstract class Lineage extends serializable with Queriable{
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

  def load(path: String, sc: SparkContext): NarrowLineage = {
    val mappingRDD = sc.objectFile[Mapping](Lineage.path+"/"+path+"/mappingRDD")
    val mRDD = mappingRDD.map(m => m match{
      case gm: GeoMapping => {
        GeoMappingWithIndex(gm)
      }
      case _ => m
    })
    mRDD.cache()
    println("index build time: "+time(mRDD.count)+"s")
    //a trivial rdd
    val rdd = sc.parallelize(Seq(1))
    //a trivial transformer
    val transformer = Transformer[Int, Int](_ * 1)

    NarrowLineage(rdd, rdd, mRDD, transformer)
  }

  def load(paths: Seq[String], sc: SparkContext): Seq[NarrowLineage] = {
    paths.map(p => {
      load(p, sc)
    })
  }

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    (System.nanoTime-s)/1e9
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
    outRDD.cache()
    val numTrials = 3
    val trialTimeList = (0 until numTrials).map(i => {
      val sampleRDD = outRDD.sample(true, 0.1)
      val path = Lineage.pathTrial+"/"+tag+"/outRDD-"+i
      sampleRDD.saveAsObjectFile(path)
      sampleRDD.unpersist()
      val sc = sampleRDD.context
      clearCache()
      val rdd = sc.objectFile(path)
      println(tag+" sampleRDD size: "+sampleRDD.count)
      time(rdd.count)
    }).toList
    val predictedLoadTime = trialTimeList.sum*10/trialTimeList.length

    val outPath = Lineage.path+"/"+tag+"/outRDD"
    outRDD.saveAsObjectFile(outPath)
    outRDD.unpersist()
    clearCache()
    val sc = outRDD.context
    val rdd = sc.objectFile(outPath)
    val loadTime = time(rdd.count)
    
    println(tag+" predictedLoadTime: "+predictedLoadTime+" actualLoadTime: "+loadTime+" trialTimeList: "+trialTimeList)  
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
    (System.nanoTime-s)/1e9
  }

  /*
   *  Helper function to clear cache on all nodes
   */
   def clearCache() = {
    Seq("bash", "-c", "for h in `cat ~/spark/conf/slaves`; do ssh $h \"free && sync && echo 3 > /proc/sys/vm/drop_caches && free\"; done") !
   }
}

/*
 *   CompositeLineage and ParallelLineage are for querying only.
 */

case class CompositeLineage(lineageSeq: Seq[NarrowLineage]) extends Queriable{
  val mappingSeq = lineageSeq.map(l => l.mappingRDD)
  val mappingRDD: RDD[Seq[Mapping]] = mappingSeq.map(rdd => rdd.map(m => Seq(m.asInstanceOf[Mapping]))).reduceLeft((x, y) => {
    x.zip(y).map(z => z._1 ++ z._2)
  })

  val mergedMappingRDD = mappingRDD.map(s => merge(s))

  def merge(inSeq: Seq[Mapping]): Seq[Mapping] = {
    inSeq.map(Seq(_)).reduceLeft((x, y) => {
      (x.last, y.head) match {
        case (m1: AllMapping, m2: GeoMapping) => x ++ y
        case (m1: GeoMapping, m2: AllMapping) => x ++ y
        //Rule: All + Any/Geo = All, Any + All/Geo = All
        case (m1: AllMapping, m2: Mapping) => x.dropRight(1) ++ Seq(AllMapping(m1.getInSpace, m2.getOutSpace))
        case (m1: Mapping, m2: AllMapping) => x.dropRight(1) ++ Seq(AllMapping(m1.getInSpace, m2.getOutSpace))
        //Rule: Identity + Any = Any, Any + Identity = Any
        case (m1: IdentityMapping, m2: Mapping) => x.dropRight(1) ++ Seq(m2)
        case (m1: Mapping, m2: IdentityMapping) => x
        //Rule: LinCom + LinCom = LinCom
        case (m1: LinComMapping, m2: LinComMapping) => Seq(LinComMapping(m1.getInSpace, m2.getOutSpace))
        case _ => x ++ y
      }
    })
  }

  def qForward(keys: List[Coor]) = {
    val i = keys.head.first
    val resultRDD = mergedMappingRDD.zipWithIndex.map{
      case (mappingSeq, index) => {
        if(index == i){
          qForwardRecursive(keys.map(_.lower), mappingSeq)
        }
      }
    }

    val filteredRDD = resultRDD.zipWithIndex.filter{
      case (result, index) => (index == i)
    }.map(_._1)

    val m = filteredRDD.first
    val innerRet = m.asInstanceOf[List[Coor]]

    if(innerRet.size == 0) 
      List(Coor())
    else
      innerRet.map(_.raise(i))
  }

  @tailrec private def qForwardRecursive(keys: List[Coor], mappingSeq: Seq[Mapping]): List[Coor] = {
    mappingSeq match {
      case Nil => keys
      case head::tail => {
        val interResults = head.qForward(keys)
        qForwardRecursive(interResults, tail)
      }
    }
  }

  def qBackward(keys: List[Coor]) = {
    val i = keys.head.first
    val resultRDD = mergedMappingRDD.zipWithIndex.map{
      case (mappingSeq, index) => {
        if(index == i){
          qBackwardRecursive(keys.map(_.lower), mappingSeq.reverse)
        }
      }
    }

    val filteredRDD = resultRDD.zipWithIndex.filter{
      case (result, index) => (index == i)
    }.map(_._1)

    val m = filteredRDD.first
    val innerRet = m.asInstanceOf[List[Coor]]

    if(innerRet.size == 0) 
      List(Coor())
    else
      innerRet.map(_.raise(i))
  }

  @tailrec private def qBackwardRecursive(keys: List[Coor], mappingSeq: Seq[Mapping]): List[Coor] = {
    mappingSeq match {
      case Nil => keys
      case head::tail => {
        val interResults = head.qBackward(keys)
        qBackwardRecursive(interResults, tail)
      }
    }
  }
}

object CompositeLineage{
  def apply(stringSeq: Seq[String], sc: SparkContext) = {
    val sl = Lineage.load(stringSeq, sc)
    new CompositeLineage(sl)
  }
}

case class ParallelLineage(compositeLineageSeq: Seq[CompositeLineage]) extends Queriable{
  def qForward(keys: List[Coor]) = {
    val groupedKeys = keys.map(k => (k.first, k.lower)).groupBy(_._1).map{
      case (k, v) => v.map(_._2)
    }

    val interRets = groupedKeys.zip(compositeLineageSeq).map{
      case (keyList, lineage) => lineage.qForward(keyList)
    }

    /*interRets.zipWithIndex.map{
      case (result, index) => result.map(_.raise(index))
    }.flatMap(identity).toList.distinct*/
    interRets.flatMap(identity).toList.distinct
  }

  def qBackward(keys: List[Coor]) = {
    val groupedKeys = keys.map(k => (k.first, k.lower)).groupBy(_._1).map{
      case (k, v) => v.map(_._2)
    }

    val interRets = groupedKeys.zip(compositeLineageSeq).map{
      case (keyList, lineage) => lineage.qBackward(keyList)
    }

    /*interRets.zipWithIndex.map{
      case (result, index) => result.map(_.raise(index))
    }.flatMap(identity).toList.distinct*/
    interRets.flatMap(identity).toList.distinct
  }
}

object ParallelLineage{
  def apply(pathMatrix: Seq[Seq[String]], sc: SparkContext) = {
    val comLineageSeq = pathMatrix.map(pathSeq => CompositeLineage(pathSeq, sc))
    new ParallelLineage(comLineageSeq)
  }
}

case class ComplexLineage(lineageSeq: Seq[Queriable]) extends Queriable{
  def qForward(keys: List[Coor]) = {
    qForwardRecursive(keys, lineageSeq)
  }

  @tailrec private def qForwardRecursive(keys: List[Coor], lineageSeq: Seq[Queriable]):List[Coor] = {
    lineageSeq match{
      case Nil => keys
      case head::tail => qForwardRecursive(head.qForward(keys), tail)
    }
  }

  def qBackward(keys: List[Coor]) = {
    qBackwardRecursive(keys, lineageSeq.reverse)
  }

  @tailrec private def qBackwardRecursive(keys: List[Coor], lineageSeq: Seq[Queriable]):List[Coor] = {
    lineageSeq match {
      case Nil => keys
      case head::tail => qBackwardRecursive(head.qBackward(keys), tail)
    }
  }
}





