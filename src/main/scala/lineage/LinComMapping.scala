package lineage

import breeze.linalg._

case class LinComMapping(inSpace: SubSpace, outSpace: SubSpace) extends Mapping(inSpace, outSpace) {
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match{
      case true => {
        //println("lincom mapping forward optimization path")
        val rule = LinComForwardQueryRule(inSpace, outSpace, keys)
        (inSpace, outSpace) match {
          case (in: Vector, out: Vector) => {
            def query = qForwardV2V(in, out, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Matrix, out: Matrix) => {
            def query = qForwardM2M(in, out, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
        }
      }
      case false => {
        //println("lincom mapping forward non-optimization path")
        (inSpace, outSpace) match {
          case (in: Vector, out: Vector) => qForwardV2V(in, out, keys)
          case (in: Matrix, out: Matrix) => qForwardM2M(in, out, keys)
        }
      }
    }
  }

  def qForwardV2V(in: Vector, out: Vector, keys: List[Coor]) = {
    out.expand()
  }

  def qForwardM2M(in: Matrix, out: Matrix, keys: List[Coor]) = {
    keys.flatMap(k=>{
      val k2D = k.asInstanceOf[Coor2D]
      val l = for (i <- 0 until out.yDim) yield Coor(k2D.x, i)
      l.toList
    }).distinct
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match{
      case true => {
        //println("lincom mapping backward optimization path")
        val rule = LinComBackwardQueryRule(inSpace, outSpace, keys)
        (inSpace, outSpace) match {
          case (in: Vector, out: Vector) => {
            def query = qForwardV2V(out, in, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Matrix, out: Matrix) => {
            def query = qForwardM2M(out, in, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
        }        
      }
      case false => {
        //println("lincom mapping backward non-optimization path")
        (inSpace, outSpace) match {
          case (in: Vector, out: Vector) => qForwardV2V(out, in, keys)
          case (in: Matrix, out: Matrix) => qForwardM2M(out, in, keys)
        }
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