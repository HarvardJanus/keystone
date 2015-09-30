package workflow

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

import java.io._
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.io.Source

/**
 *  Each Lineage corresponds to one transformer
 *  @param inRDD input RDD of the transformer
 *  @param outRDD output RDD of the transformer
 *  @param mappingRDD each item of mappingRDD corresponds to one item in inRDD and one item in outRDD
 *  @param modelRDD related models of transformer, it can be a random seed, a vector or a matrix
 *  @param transformer the transformer itself
 */

abstract class Lineage(modelRDD: Option[_]) extends serializable{
  val path = "Lineage"
  //qForward() and qBackward() methods need implementation, should call to mappingRDD
  def qForward(key: Option[_]):List[_]
  def qBackward(key: Option[_]):List[_]
  def save(tag: String)
  def size: Long
}

case class NarrowLineage(inRDD: RDD[_], outRDD: RDD[_], mappingRDD: RDD[_], transformer: Transformer[_,_], 
  modelRDD: Option[_]) extends Lineage(modelRDD){

  def qForward(key: Option[_]) = {
    key.getOrElse(null) match{
      case (i:Int, j:Int) => {
        val resultRDD = mappingRDD.zipWithIndex.map{
          case (mapping, index) => {
            if(index == i){
              mapping.asInstanceOf[Mapping].qForward(Some(j))
            }
          }
        }
        /* Need to check the reason for empty resultRDD, 
         * if it is out of RDD boundary, raise an exception.
         * If it is really empty, return an empty list.*/
        val filteredRDD = resultRDD.zipWithIndex.filter{
          case (result, index) => (index == i)
        }.map(x => x._1)
        val m = filteredRDD.first
        val innerRet = m.asInstanceOf[List[_]]
        List.fill(innerRet.size){i}.zip(innerRet)
      }
      case (i:Int, j:Int, k:Int) => {
        val resultRDD = mappingRDD.zipWithIndex.map{
          case (mapping, index) => {
            if(index == i){
              mapping.asInstanceOf[Mapping].qForward(Some(j,k))
            }
          }
        }
        val filteredRDD = resultRDD.zipWithIndex.filter{
          case (result, index) => (index == i)
        }.map(x => x._1)
        val m = filteredRDD.first
        val innerRet = m.asInstanceOf[List[_]]
        List.fill(innerRet.size){i}.zip(innerRet).map(v => Lineage.flatten(v))
      }
      case (i:Int, j:Int, k:Int, c:Int) => {
        val resultRDD = mappingRDD.zipWithIndex.map{
          case (mapping, index) => {
            if(index == i){
              mapping.asInstanceOf[Mapping].qForward(Some(j,k,c))
            }
          }
        }
        val filteredRDD = resultRDD.zipWithIndex.filter{
          case (result, index) => (index == i)
        }.map(x => x._1)
        val m = filteredRDD.first
        val innerRet = m.asInstanceOf[List[_]]
        List.fill(innerRet.size){i}.zip(innerRet).map(v => Lineage.flatten(v))
      }
    }
  }

  def qBackward(key: Option[_]) = {
    key.getOrElse(null) match{
      case (i:Int, j:Int) => {
        val resultRDD = mappingRDD.zipWithIndex.map{
          case (mapping, index) => {
            if(index == i){
              mapping.asInstanceOf[Mapping].qBackward(Some(j))
            }
          }
        }
        val filteredRDD = resultRDD.zipWithIndex.filter{
          case (result, index) => (index == i)
        }.map(x => x._1)
        val m = filteredRDD.first
        val innerRet = m.asInstanceOf[List[_]]
        List.fill(innerRet.size){i}.zip(innerRet)
      }
      case (i:Int, j:Int, k:Int) => {
        val resultRDD = mappingRDD.zipWithIndex.map{
          case (mapping, index) => {
            if(index == i){
              mapping.asInstanceOf[Mapping].qBackward(Some(j,k))
            }
          }
        }
        val filteredRDD = resultRDD.zipWithIndex.filter{
          case (result, index) => (index == i)
        }.map(x => x._1)
        val m = filteredRDD.first
        val innerRet = m.asInstanceOf[List[_]]
        List.fill(innerRet.size){i}.zip(innerRet).map(v => Lineage.flatten(v))
      }
      case (i:Int, j:Int, k:Int, c:Int) => {
        val resultRDD = mappingRDD.zipWithIndex.map{
          case (mapping, index) => {
            if(index == i){
              mapping.asInstanceOf[Mapping].qBackward(Some(j,k,c))
            }
          }
        }
        val filteredRDD = resultRDD.zipWithIndex.filter{
          case (result, index) => (index == i)
        }.map(x => x._1)
        val m = filteredRDD.first
        val innerRet = m.asInstanceOf[List[_]]
        List.fill(innerRet.size){i}.zip(innerRet).map(v => Lineage.flatten(v))
      }
    }
  }

  def qForwardBatch(keyList: List[_]) = {
    val layeredKeyList:List[(Int, Option[_])] = keyList.map(k => {
      k match {
        case (i:Int, j:Int) => (i,Some(j))
        case (i:Int, j:Int, k:Int) => (i, Some(j,k))
        case (i:Int, j:Int, k:Int, c:Int) => (i, Some(j,k,c))
      }
    })
    val sampleKey = layeredKeyList(0)
    val sampleI = sampleKey.asInstanceOf[(Int, _)]._1

    val resultRDD = mappingRDD.zipWithIndex.map{
      case (mapping, index) => {
        if(index == sampleI) mapping.asInstanceOf[Mapping].qForwardBatch(layeredKeyList.map(_._2)).flatMap(identity)
      }
    }

    val filteredRDD = resultRDD.zipWithIndex.filter{
      case (result, index) => (index == sampleI)
    }.map(_._1)

    val innerRet = filteredRDD.first.asInstanceOf[List[_]]
    List.fill(innerRet.size){sampleI}.zip(innerRet).map(Lineage.flatten(_))
  }

  def qBackwardBatch(keyList: List[_]) = {
    val layeredKeyList:List[(Int, Option[_])] = keyList.map(k => {
      k match {
        case (i:Int, j:Int) => (i,Some(j))
        case (i:Int, j:Int, k:Int) => (i, Some(j,k))
        case (i:Int, j:Int, k:Int, c:Int) => (i, Some(j,k,c))
      }
    })
    val sampleKey = layeredKeyList(0)
    val sampleI = sampleKey.asInstanceOf[(Int, _)]._1

    val resultRDD = mappingRDD.zipWithIndex.map{
      case (mapping, index) => {
        if(index == sampleI) mapping.asInstanceOf[Mapping].qBackwardBatch(layeredKeyList.map(_._2)).flatMap(identity)
      }
    }

    val filteredRDD = resultRDD.zipWithIndex.filter{
      case (result, index) => (index == sampleI)
    }.map(_._1)

    val innerRet = filteredRDD.first.asInstanceOf[List[_]]
    List.fill(innerRet.size){sampleI}.zip(innerRet).map(Lineage.flatten(_))
  }

  def save(tag: String) = {
    val context = mappingRDD.context
    val tRDD = context.parallelize(Seq(transformer), 1)
    tRDD.saveAsObjectFile(path+"/"+tag+"/transformerRDD")

    //save RDD to disk with possible optimization on identical items
    /*if(mappingRDD.distinct.count == 1){
      val mapping = mappingRDD.first
      context.parallelize(Seq(mapping), 1).saveAsObjectFile(path+"/"+tag+"/mappingRDD")
    }
    else{
      mappingRDD.saveAsObjectFile(path+"/"+tag+"/mappingRDD")
    }*/
    mappingRDD.saveAsObjectFile(path+"/"+tag+"/mappingRDD")
    /*val words = tag.split('_')
    if(words.size > 1 && words(1).startsWith("0")){
      inRDD.saveAsObjectFile(path+"/"+tag+"/inRDD")
    }
    outRDD.saveAsObjectFile(path+"/"+tag+"/outRDD")*/
  }
  def size = mappingRDD.count
}

class GatherLineage(inSeq: Seq[RDD[_]], outRDD: RDD[_], mapping: TransposeMapping, transformer: GatherTransformer[_], 
  modelRDD: Option[_]) extends Lineage(modelRDD){

  def qForward(key: Option[_]) = {
    key.getOrElse(null) match{
      case (i:Int, j:Int, k:Int) => {
        val innerRet = mapping.qForward(Some((i, j)))
        val list = innerRet.zip(List.fill(innerRet.size){k})
        list.map(x => (x._1._1, x._1._2, x._2))
      }
    }
  }

  def qBackward(key: Option[_]) = {
    key.getOrElse(null) match{
      case (i:Int, j:Int, k:Int) => {
        val innerRet = mapping.qBackward(Some((i, j)))
        val list = innerRet.zip(List.fill(innerRet.size){k})
        list.map(x => (x._1._1, x._1._2, x._2))
      }
    }
  }

  def save(tag: String) = {
    val context = outRDD.context
    val rdd = context.parallelize(Seq(transformer), 1)
    rdd.saveAsObjectFile(path+"/"+tag+"/transformerRDD")
    val mrdd = context.parallelize(Seq(mapping), 1)
    mrdd.saveAsObjectFile(path+"/"+tag+"/mappingRDD")
  }
  def size = inSeq.size
}

class SampleLineage(inRDD: RDD[_], outRDD: RDD[_], mappingRDD: RDD[_], transformer: Transformer[_,_], 
  modelRDD: Option[_]) extends NarrowLineage(inRDD, outRDD, mappingRDD, transformer, modelRDD){
  
  override def qForward(key: Option[_]) = {
    val interResult = super.qForward(key)
    key.getOrElse(null) match{
      case (i:Int, j:Int, k:Int) => List(interResult(j))
    }
  }

  override def qBackward(key: Option[_]) = {
    val interResult = super.qBackward(key)
    key.getOrElse(null) match{
      case (i:Int, j:Int, k:Int) => List(interResult(j))
    }
  }
}

class PipelineLineage(lineageList: List[NarrowLineage]){
  def qForward(keys: List[_], list: List[NarrowLineage]=lineageList): List[_] = {
    list match {
      case first::rest => {
        val innerResults = keys.flatMap(key => first.qForward(Some(key)))
        if(rest.isEmpty){
          innerResults
        }
        else{
          qForward(innerResults, rest)
        }
      }
      case List() => List()
    }
  }

  def qBackward(keys: List[_], list: List[NarrowLineage]=lineageList.reverse): List[_] = {
    list match {
      case first::rest => {
        val innerResults = keys.flatMap(key => first.qBackward(Some(key)))
        if(rest.isEmpty){
          innerResults
        }
        else{
          qBackward(innerResults, rest)
        }
      }
      case List() => List()
    }
  }

  def qForwardBatch(keyList: List[_], list: List[NarrowLineage] = lineageList) = {
    val innerKeyList = keyList.map(k => {
      k match {
        case (i:Int, j:Int) => Some(j)
        case (i:Int, j:Int, k:Int) => Some(j,k)
        case (i:Int, j:Int, k:Int, c:Int) => Some(j,k,c)
      }
    })
    val sampleKey = keyList(0)
    val sampleI:Int = sampleKey match {
      case (i,j) => i.asInstanceOf[Int]
      case (i,j,k) => i.asInstanceOf[Int]
      case (i,j,k,c) => i.asInstanceOf[Int]
    }

    val mappingRDDList: List[RDD[Mapping]] = list.map(_.mappingRDD.asInstanceOf[RDD[Mapping]])

    val innerRet = qForwardBatchRecursive(innerKeyList, mappingRDDList, sampleI)
    val layeredRet = List.fill(innerRet.size){sampleI}.zip(innerRet.map(_.get))
    layeredRet.map(Lineage.flatten(_))
  }

  def qForwardBatchRecursive(keyList: List[Option[_]], list: List[RDD[Mapping]], sampleI:Int):List[Option[_]] = {
    list match{
      case head::tail => {
        val resultsRDD = head.zipWithIndex.map{
          case (mapping, index) => {
            if(index == sampleI){
              mapping.qForwardBatch(keyList).flatMap(identity)
            }
          }
        }

        val filteredRDD = resultsRDD.zipWithIndex.filter{
          case (result, index) => (index == sampleI)
        }.map(_._1)

        val innerRet = filteredRDD.first.asInstanceOf[List[_]]
        val results = innerRet.map(Some(_))

        if(tail.isEmpty){
          results
        }
        else{
          qForwardBatchRecursive(results, tail, sampleI)
        }
      }
      case Nil => keyList
    }
  }

  def qBackwardBatch(keyList: List[_], list: List[NarrowLineage] = lineageList.reverse) = {
    val innerKeyList = keyList.map(k => {
      k match {
        case (i:Int, j:Int) => Some(j)
        case (i:Int, j:Int, k:Int) => Some(j,k)
        case (i:Int, j:Int, k:Int, c:Int) => Some(j,k,c)
      }
    })
    val sampleKey = keyList(0)
    val sampleI:Int = sampleKey match {
      case (i,j) => i.asInstanceOf[Int]
      case (i,j,k) => i.asInstanceOf[Int]
      case (i,j,k,c) => i.asInstanceOf[Int]
    }

    val mappingRDDList: List[RDD[Mapping]] = list.map(_.mappingRDD.asInstanceOf[RDD[Mapping]])

    val innerRet = qBackwardBatchRecursive(innerKeyList, mappingRDDList, sampleI)

    val layeredRet = List.fill(innerRet.size){sampleI}.zip(innerRet.map(_.get))
    layeredRet.map(Lineage.flatten(_))
  }

  def qBackwardBatchRecursive(keyList: List[Option[_]], list: List[RDD[Mapping]], sampleI:Int):List[Option[_]] = {
    list match{
      case head::tail => {
        val resultsRDD = head.zipWithIndex.map{
          case (mapping, index) => {
            if(index == sampleI){
              mapping.qBackwardBatch(keyList).flatMap(identity)
            }
          }
        }

        val filteredRDD = resultsRDD.zipWithIndex.filter{
          case (result, index) => (index == sampleI)
        }.map(_._1)

        val innerRet = filteredRDD.first.asInstanceOf[List[_]]
        val results = innerRet.map(Some(_))

        if(tail.isEmpty){
          results
        }
        else{
          qBackwardBatchRecursive(results, tail, sampleI)
        }
      }
      case Nil => keyList
    }
  }
}

object PipelineLineage{
  def apply(paths: List[String], sc: SparkContext) = {
    val lineageList = paths.map(p => {
      val s = sc.parallelize(Seq(1))
      val transformer = Transformer[Int, Int](_ * 1)
      val rdd = sc.objectFile[Mapping](p+"/mappingRDD")
      rdd.cache
      new NarrowLineage(s, s, rdd, transformer, Some(None))
      })
    new PipelineLineage(lineageList)
  }
}

object Lineage{
  implicit def intToOption(key: Int): Option[Int] = Some(key)
  implicit def int2DToOption(key: (Int, Int)): Option[(Int, Int)] = Some(key)
  implicit def int3DToOption(key: (Int, Int, Int)): Option[(Int, Int, Int)] = Some(key)
  implicit def int4DToOption(key: (Int, Int, Int, Int)): Option[(Int, Int, Int, Int)] = Some(key)
  implicit def indexInt2DToOption(key: (Int, (Int, Int))): Option[(Int, (Int, Int))] = Some(key)

  def flatten[T](in: (Int,T)) = {
    in match {
      case (i: Int, j: Int) => (i, j)
      case (i: Int, (j: Int, k: Int)) => (i, j, k)
      case (i: Int, (j: Int, k: Int, c: Int)) => (i, j, k, c)
    }
  }

  //need a lineage apply interface to reconstruct the lineage object from files on disk
  def load(mappingRDD: RDD[Mapping], transformerRDD: RDD[_]): NarrowLineage = {
    val sc = mappingRDD.context
    val rdd = sc.parallelize(Seq(1))
    val transformer = transformerRDD.first.asInstanceOf[Transformer[_,_]]
    new NarrowLineage(rdd, rdd, mappingRDD, transformer, Some(None))
  }

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    (System.nanoTime-s)/1e6
  }

  def loadAndQuerySeq(mappingRDD: RDD[Mapping], xDim: Int, yDim: Int) = {
    val sc = mappingRDD.context
    mappingRDD.cache

    //trivial rdd
    val rdd = sc.parallelize(Seq(1))
    //trivial transformer
    val transformer = Transformer[Int, Int](_ * 1)
    val lineage = new NarrowLineage(rdd, rdd, mappingRDD, transformer, Some(None))

    val count = mappingRDD.count.toInt
    val timeVector = for(itemID <- (0 until count/100); r <- (0 until xDim); j<- (0 until yDim))
      yield(time(lineage.qBackward(itemID, r, j)))

    timeVector.toList
  }

  def loadAndQueryBatch(paths: List[String], numQuery: Int, sc: SparkContext) = {
    val lineage = PipelineLineage(paths, sc)
    lineage.qBackward(List(0,0,0))
    lineage.qBackward(List(0,0,0))
    val list = List.fill(numQuery){(0,0)}.zip((0 until numQuery)).map(x=> (x._1._1, x._1._2, x._2))
    time(lineage.qBackwardBatch(list))
  }
}


object OneToOneLineage{
  def apply(in: RDD[_], out:RDD[_], transformer: Transformer[_, _], model: Option[_] = None) = {
    val mapping = in.zip(out).map({
      case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
        new ElementMapping(Metadata(vIn.size), Metadata(vOut.size))
      }
      case (sIn: Seq[_], vOut: DenseVector[_]) => {
        //sIn should be Seq[DenseVetor[_]]
        val sampleInVector = sIn(0).asInstanceOf[DenseVector[_]]
        new ElementMapping(Metadata(sIn.size,sampleInVector.size), Metadata(vOut.size))
      }
      case (mIn: DenseMatrix[_], mOut: DenseMatrix[_]) => {
        new ElementMapping(Metadata(mIn.rows, mIn.cols), Metadata(mOut.rows, mOut.cols))
      }
      case (mIn: DenseMatrix[_], mOut: DenseVector[_]) => {
        new ElementMapping(Metadata(mIn.rows, mIn.cols), Metadata(mOut.size))
      }
      case (imageIn: MultiLabeledImage, imageOut: Image) => {
        new ElementMapping(Metadata(imageIn.image.metadata), Metadata(imageOut.metadata))
      }
      case (imageIn: Image, imageOut: Image) => {
        new ElementMapping(Metadata(imageIn.metadata), Metadata(imageOut.metadata))
      }
      case _ => None
    })
    new NarrowLineage(in, out, mapping, transformer, model)
  }
}

object AllToOneLineage{
  def apply(in: RDD[_], out:RDD[_], transformer: Transformer[_, _], model: Option[_] = None) = {
    val mapping = in.zip(out).map({
      case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
        new AllMapping(Metadata(vIn.size), Metadata(vOut.size))
      }
      case (vIn: DenseVector[_], vOut: Int) => {
        new AllMapping(Metadata(vIn.size), Metadata(1))
      }
      case (vIn: DenseMatrix[_], vOut: DenseMatrix[_]) => {
        new AllMapping(Metadata(vIn.rows, vIn.cols), Metadata(vOut.rows, vOut.cols))
      }
      case (vIn: Image, vOut: Image) => {
        new AllMapping(Metadata(vIn.metadata), Metadata(vOut.metadata))
      }
      case _ => None
    })
    new NarrowLineage(in, out, mapping, transformer, model)
  }
}

object LinComLineage{
  def apply[T](in: RDD[_], out:RDD[_], transformer: Transformer[_, _], model: Option[DenseMatrix[T]]) = {
    val m = model.getOrElse(None).asInstanceOf[DenseMatrix[T]]
    val mapping = in.zip(out).map({
      case (sIn: DenseVector[_], sOut: DenseVector[_]) => {
        new LinComMapping(VectorMeta(sIn.size), VectorMeta(sOut.size), MatrixMeta(m.rows, m.cols))
      }
      case (sIn: DenseMatrix[_], sOut: DenseMatrix[_]) => {
        new LinComMapping(MatrixMeta(sIn.rows, sIn.cols), MatrixMeta(sOut.rows, sOut.cols), MatrixMeta(m.rows, m.cols))
      }
      case _ => None
    })
    new NarrowLineage(in, out, mapping, transformer, model)
  }
}

object RegionLineage{
  /*def apply(in: RDD[_], out: RDD[_], ioList: RDD[List[(List[(Int, Int)], List[(Int, Int)])]], 
    transformer: Transformer[_, _], model: Option[_]=None) = {
    val mapping = ioList.map(l => ContourMapping(l))
    //val mapping = ioList.map(l => OneManyMapping(l))
    //val mapping = ioList.map(l => SimpleMapping(l))

    new NarrowLineage(in, out, mapping, transformer, model)
  }*/

  def apply(in: RDD[_], out: RDD[_], ioList: RDD[List[(Shape, Shape)]], 
    transformer: Transformer[_, _], model: Option[_]=None) = {
    val mapping = ioList.map(l => ContourMapping(l))
    //val mapping = ioList.map(l => OneManyMapping(l))
    //val mapping = ioList.map(l => SimpleMapping(l))

    new NarrowLineage(in, out, mapping, transformer, model)
  }
}

object SampleLineage{
  def apply(in: RDD[_], out: RDD[_], ioList: RDD[List[(Shape, Shape)]], 
    transformer: Transformer[_, _], model: Option[_]=None) = {
    val mapping = ioList.map(l => ContourMapping(l))
    new SampleLineage(in, out, mapping, transformer, model)
  }
}

object GatherLineage{
  def apply[T](in: Seq[RDD[T]], out: RDD[Seq[T]], transformer: GatherTransformer[_], model: Option[_]=None) = {
    val sampleIn = in(0)
    val sampleOut = out.first
    val mapping = TransposeMapping(in.size, sampleIn.count, out.count, sampleOut.size)
    new GatherLineage(in, out, mapping, transformer, model)
  }
}
