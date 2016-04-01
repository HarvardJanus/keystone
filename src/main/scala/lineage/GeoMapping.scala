package lineage

import breeze.linalg._
import com.github.davidmoten.rtree.geometry._
import com.github.davidmoten.rtree.geometry.Geometries._
import com.github.davidmoten.rtree.RTree
import scala.collection.JavaConversions._
import utils.{MultiLabeledImage, Image=>KeystoneImage, ImageMetadata, LabeledImage}

case class GeoMapping(inSpace: SubSpace, outSpace: SubSpace, tupleList: List[(Shape, Shape)]) extends Mapping(inSpace, outSpace){

  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match {
      case true => {
        val rule = GeoForwardQueryRule(inSpace, outSpace, tupleList, keys)
        def query = qForwardAdaptive(_: List[Coor])
        QueryRule.optimizeThenQuery(rule, query)
      }
      case false => {
        qForwardAdaptive(keys)
      }
    }
  }

  def qForwardAdaptive(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k: Coor2D => {
          val filteredList = tupleList.filter(t => t._1.contain(k.x.toDouble, k.y.toDouble))
          filteredList.flatMap(t => t._2.toCoor)
        }
      }
    }).distinct
  }
  

  def qBackwardAdaptive(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k: Coor2D => {
          val filteredList = tupleList.filter(t => t._2.contain(k.x.toDouble, k.y.toDouble))
          filteredList.flatMap(t => t._1.toCoor)
        }
      }
    }).distinct
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match {
      case true => {
        val rule = GeoBackwardQueryRule(inSpace, outSpace, tupleList, keys)
        def query = qBackwardAdaptive(_: List[Coor])
        QueryRule.optimizeThenQuery(rule, query)
      }
      case false => {
        qBackwardAdaptive(keys)
      }
    }
  }
}

case class GeoMappingWithIndex(inSpace: SubSpace, outSpace: SubSpace, fRTree: RTree[Int, Rectangle], 
  bRTree: RTree[Int, Rectangle], tupleList: List[(Shape, Shape)]) extends Mapping(inSpace, outSpace){
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match {
      case true => {
        val rule = GeoForwardQueryRule(inSpace, outSpace, tupleList, keys)
        def query = qForwardAdaptive(_: List[Coor])
        QueryRule.optimizeThenQuery(rule, query)
      }
      case false => {
        qForwardAdaptive(keys)
      }
    }
  }

  def qForwardAdaptive(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k: Coor2D => {
          val indexArray = fRTree.search(point(k.x.toDouble, k.y.toDouble)).toBlocking().toIterable()
          val indexList = indexArray.toList
          val filteredIndex = indexList.toList.filter(e => tupleList(e.value)._1.contain(k.x.toDouble, k.y.toDouble))
          filteredIndex.flatMap(e => tupleList(e.value)._2.toCoor)
        }
      }
    }).distinct
  }
  

  def qBackwardAdaptive(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k: Coor2D => {
          val indexArray = bRTree.search(point(k.x.toDouble, k.y.toDouble)).toBlocking().toIterable()
          val indexList = indexArray.toList
          val filteredIndex = indexList.toList.filter(e => tupleList(e.value)._2.contain(k.x.toDouble, k.y.toDouble))
          filteredIndex.flatMap(e => tupleList(e.value)._1.toCoor)
        }
      }
    }).distinct
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match {
      case true => {
        val rule = GeoBackwardQueryRule(inSpace, outSpace, tupleList, keys)
        def query = qBackwardAdaptive(_: List[Coor])
        QueryRule.optimizeThenQuery(rule, query)
      }
      case false => {
        qBackwardAdaptive(keys)
      }
    }
  }
}

object GeoMapping{
  def apply(inMatrix: DenseMatrix[_], outMatrix: DenseMatrix[_], tupleList: List[(Shape, Shape)]) = {
    //val (fRTree, bRTree) = buildRTreeIndex(tupleList)
    //new GeoMapping(SubSpace(inMatrix), SubSpace(outMatrix), fRTree, bRTree, tupleList)
    new GeoMapping(SubSpace(inMatrix), SubSpace(outMatrix), tupleList)
  }
  
  def apply(inImage: KeystoneImage, outMatrix: DenseMatrix[_], tupleList: List[(Shape, Shape)]) = {
    new GeoMapping(SubSpace(inImage), SubSpace(outMatrix), tupleList)
  }
}

object GeoMappingWithIndex{
  def apply(gm: GeoMapping) = {
    val (fRTree, bRTree) = buildRTreeIndex(gm.tupleList)
    new GeoMappingWithIndex(gm.inSpace, gm.outSpace, fRTree, bRTree, gm.tupleList)
  }

  def buildRTreeIndex(tupleList: List[(Shape, Shape)]): (RTree[Int, Rectangle], RTree[Int, Rectangle]) = {
    var fRTree: RTree[Int, Rectangle] = RTree.create()
    var bRTree: RTree[Int, Rectangle] = RTree.create()

    /*need to change to automatic shape detection*/
    val maps = tupleList.zipWithIndex.map{
      case ((s1: Shape, s2: Shape), i: Int) => {
        fRTree = fRTree.add(i, s1.toBox)
        bRTree = bRTree.add(i, s2.toBox)
      }
    }
    (fRTree, bRTree)
  }
}