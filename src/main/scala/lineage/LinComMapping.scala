package lineage

import breeze.linalg._

case class LinComMapping(inSpace: SubSpace, outSpace: SubSpace) extends Mapping {
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Vector, out: Vector) => out.expand()
      case (in: Matrix, out: Matrix) => {
        keys.flatMap(k=>{
          val k2D = k.asInstanceOf[Coor2D]
          val l = for (i <- 0 until out.yDim) yield Coor(k2D.x, i)
          l.toList
        })
      }
    }
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Vector, out: Vector) => in.expand()
      case (in: Matrix, out: Matrix) => {
        keys.flatMap(k=>{
          val k2D = k.asInstanceOf[Coor2D]
          val l = for (i <- 0 until in.yDim) yield Coor(k2D.x, i)
          l.toList
        })
      }
    }
  }
}

object LinComMapping{
  def apply(inVector: DenseVector[_], outVector: DenseVector[_]) = 
    new LinComMapping(SubSpace(inVector), SubSpace(outVector))
  def apply(inMatrix: DenseMatrix[_], outMatrix: DenseMatrix[_]) = 
    new LinComMapping(SubSpace(inMatrix), SubSpace(outMatrix))
}