package lineage

import archery._

case class GeoMapping(fRTree: RTree[Int], bRTree: RTree[Int],
  tupleList: List[(Shape, Shape)]) extends Mapping{

  def qForward(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k: Coor2D => {
          val indexArray = fRTree.searchWithIn(Point(k.x.toFloat, k.y.toFloat))
          val filteredIndex = indexArray.toList.filter(e => tupleList(e.value)._1.contain(k.x.toDouble, k.y.toDouble))
          filteredIndex.flatMap(e => tupleList(e.value)._2.toCoor)
        }
      }  
    })
  }

  def qBackward(keys: List[Coor]) = {
    keys.flatMap(key => {
      key match {
        case k: Coor2D => {
          val indexArray = bRTree.searchWithIn(Point(k.x.toFloat, k.y.toFloat))
          val filteredIndex = indexArray.toList.filter(e => tupleList(e.value)._2.contain(k.x.toDouble, k.y.toDouble))
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
  def buildRTreeIndex(tupleList: List[(Shape, Shape)]): (RTree[Int], RTree[Int]) = {
    var fRTree: RTree[Int] = RTree()
    var bRTree: RTree[Int] = RTree()

    /*need to change to automatic shape detection*/
    val maps = tupleList.zipWithIndex.map{
      case ((s1: Shape, s2: Shape), i: Int) => {
        fRTree = fRTree.insert(Entry(s1.toBox, i))
        bRTree = bRTree.insert(Entry(s2.toBox, i))
      }
    }
    (fRTree, bRTree)
  }
}