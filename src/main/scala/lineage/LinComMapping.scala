package lineage

import breeze.linalg._

case class LinComMapping(inSpace: SubSpace, outSpace: SubSpace) extends Mapping {
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Vector, out: Vector) => out.expand()
    }
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Vector, out: Vector) => in.expand()
    }
  }
}