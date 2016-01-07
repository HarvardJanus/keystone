package lineage

import com.github.davidmoten.rtree.geometry._
import com.github.davidmoten.rtree.geometry.Geometries._
import com.github.davidmoten.rtree.RTree
import scala.collection.JavaConversions._

case class GeoMapping(fRTree: RTree[Int, Rectangle], bRTree: RTree[Int, Rectangle],
  tupleList: List[(Shape, Shape)]) extends Mapping{

  def qForward(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k: Coor2D => {
          val indexArray = fRTree.search(point(k.x.toDouble, k.y.toDouble)).toBlocking().toIterable()
          val indexList = indexArray.toList
          val filteredIndex = indexList.toList.filter(e => tupleList(e.value)._1.contain(k.x.toDouble, k.y.toDouble))
          filteredIndex.flatMap(e => tupleList(e.value)._2.toCoor)
        }
      }  
    })
  }

  def qBackward(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k: Coor2D => {
          val indexArray = bRTree.search(point(k.x.toDouble, k.y.toDouble)).toBlocking().toIterable()
          val indexList = indexArray.toList
          val filteredIndex = indexList.toList.filter(e => tupleList(e.value)._2.contain(k.x.toDouble, k.y.toDouble))
          filteredIndex.flatMap(e => tupleList(e.value)._1.toCoor)
        }
      }  
    })
  }
}

object GeoMapping{
  def apply(tupleList: List[(Shape, Shape)]) = {
    val (fRTree, bRTree) = buildRTreeIndex(tupleList)
    new GeoMapping(fRTree, bRTree, tupleList)
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