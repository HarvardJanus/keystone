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

class NarrowLineage(inRDD: RDD[_], outRDD: RDD[_], mappingRDD: RDD[_], transformer: Transformer[_,_], 
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
        List.fill(innerRet.size){i}.zip(innerRet)
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
        List.fill(innerRet.size){i}.zip(innerRet)
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
        List.fill(innerRet.size){i}.zip(innerRet)
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
        List.fill(innerRet.size){i}.zip(innerRet)
      }
    }
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
      case (i:Int, j:Int, k:Int) => {
        interResult.map{
          case (itemID: Int, list:List[_]) => (itemID, list(j))
        }
      }
    }
  }

  override def qBackward(key: Option[_]) = {
    val interResult = super.qBackward(key)
    key.getOrElse(null) match{
      case (i:Int, j:Int, k:Int) => {
        interResult.map{
          case (itemID: Int, list:List[_]) => (itemID, list(j))
        }
      }
    }
  }
}

object Lineage{
  implicit def intToOption(key: Int): Option[Int] = Some(key)
  implicit def int2DToOption(key: (Int, Int)): Option[(Int, Int)] = Some(key)
  implicit def int3DToOption(key: (Int, Int, Int)): Option[(Int, Int, Int)] = Some(key)
  implicit def int4DToOption(key: (Int, Int, Int, Int)): Option[(Int, Int, Int, Int)] = Some(key)
  implicit def indexInt2DToOption(key: (Int, (Int, Int))): Option[(Int, (Int, Int))] = Some(key)

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
}

object OneToOneLineage{
  def apply(in: RDD[_], out:RDD[_], transformer: Transformer[_, _], model: Option[_] = None) = {
    val mapping = in.zip(out).map({
      case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
        new OneToOneMapping(vIn.size, 1, vOut.size, 1, 1)
      }
      case (sIn: Seq[_], vOut: DenseVector[_]) => {
        //sIn should be Seq[DenseVetor[_]]
        val sampleInVector = sIn(0).asInstanceOf[DenseVector[_]]
        new OneToOneMapping(sampleInVector.size, 1, vOut.size, 1, sIn.size)
      }
      case (mIn: DenseMatrix[_], mOut: DenseMatrix[_]) => {
        new OneToOneMapping(mIn.rows, mIn.cols, mOut.rows, mOut.cols, 1)
      }
      case (mIn: DenseMatrix[_], mOut: DenseVector[_]) => {
        new OneToOneMapping(mIn.rows, mIn.cols, mOut.size, 1, 1)
      }
      case (imageIn: MultiLabeledImage, imageOut: Image) => {
        new OneToOneMapping(imageIn.image.flatSize, 1, imageOut.flatSize, 1, 1, Some(imageIn.image.metadata), Some(imageOut.metadata))
      }
      case (imageIn: Image, imageOut: Image) => {
        new OneToOneMapping(imageIn.flatSize, 1, imageOut.flatSize, 1, 1, Some(imageIn.metadata), Some(imageOut.metadata))
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
        new AllToOneMapping(vIn.size, 1, vOut.size, 1)
      }
      case (vIn: DenseVector[_], vOut: Int) => {
        new AllToOneMapping(vIn.size, 1, 1, 1)
      }
      case (vIn: DenseMatrix[_], vOut: DenseMatrix[_]) => {
        new AllToOneMapping(vIn.rows, vIn.cols, vOut.rows, vOut.cols)
      }
      case (vIn: Image, vOut: Image) => {
        new AllToOneMapping(vIn.flatSize, 1, vOut.flatSize, 1, Some(vIn.metadata))
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
        new LinComMapping(sIn.size, 1, sOut.size, 1, m.rows, m.cols)
      }
      case (sIn: DenseMatrix[_], sOut: DenseMatrix[_]) => {
        new LinComMapping(sIn.rows, sIn.cols, sOut.rows, sOut.cols, m.rows, m.cols)
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
