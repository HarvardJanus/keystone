package lineage

trait QueryRule{
  def isTotal(): Boolean
  def reduce(): List[Coor]
}

object QueryRule{
  def optimizeThenQuery(rule: QueryRule, query: List[Coor] => List[Coor]):List[Coor] = {
    if(rule.isTotal){
      //println("rule optimizer: totality")
      rule match {
        case r: CollapseForwardQueryRule => r.outSpace.expand()
        case r: CollapseBackwardQueryRule => r.inSpace.expand()
        case r: FlattenForwardQueryRule => r.outSpace.expand()
        case r: FlattenBackwardQueryRule => r.inSpace.expand()
        case r: LinComForwardQueryRule => r.outSpace.expand()
        case r: LinComBackwardQueryRule => r.inSpace.expand()
        case r: GeoForwardQueryRule => r.tupleList.flatMap(_._2.toCoor()).distinct //r.outSpace.expand() //r.tupleList.flatMap(_._2.toCoor()).distinct
        case r: GeoBackwardQueryRule => r.tupleList.flatMap(_._1.toCoor()).distinct //r.inSpace.expand() //r.tupleList.flatMap(_._1.toCoor()).distinct
      }
    }
    else{
      //println("rule optimizer: reduction")
      //println("reduced keys: "+rule.reduce)
      query(rule.reduce)
    }
  }
}

case class CollapseForwardQueryRule(inSpace: SubSpace, outSpace: SubSpace, dim: Int, keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = {
    (inSpace, outSpace) match {
      case (in: Vector, out: Singularity) => List(Coor(0))
      case (in: Matrix, out: Vector) => keys.map( k => {
        dim match {
          case 0 => Coor(0, k.asInstanceOf[Coor2D].y)
          case 1 => Coor(k.asInstanceOf[Coor2D].x, 0)
        }
      })
      case (in: Image, out: Matrix) => keys.map( k => {
        dim match {
          case 0 => Coor(0, k.asInstanceOf[Coor3D].y, k.asInstanceOf[Coor3D].c)
          case 1 => Coor(k.asInstanceOf[Coor3D].x, 0, k.asInstanceOf[Coor3D].c)
          case 2 => Coor(k.asInstanceOf[Coor3D].x, k.asInstanceOf[Coor3D].y, 0)
        }
      })
      case _ => keys
    }
  }.toList.distinct

  def isTotal() = {
    keys.size == inSpace.expand().size
  }

  def reduce() = reducedKeys
}

case class CollapseBackwardQueryRule(inSpace: SubSpace, outSpace: SubSpace, dim: Int, keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = keys.distinct

  def isTotal() = {
    keys.size == inSpace.expand().size
  }

  def reduce() = reducedKeys
}

case class FlattenForwardQueryRule(inSpace: SubSpace, outSpace: SubSpace, dim: Int, keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = keys.distinct

  def isTotal() = {
    keys.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}

case class FlattenBackwardQueryRule(inSpace: SubSpace, outSpace: SubSpace, dim: Int, keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = keys.distinct

  def isTotal() = {
    keys.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}

case class IdentityQueryRule(inSpace: SubSpace, outSpace: SubSpace, keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = keys.distinct

  def isTotal() = {
    keys.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}

case class AllQueryRule(inSpace: SubSpace, outSpace: SubSpace, keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = keys.distinct

  def isTotal() = true

  def reduce = reducedKeys
}

case class LinComForwardQueryRule(inSpace: SubSpace, outSpace: SubSpace, keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = {
    (inSpace, outSpace) match {
      case (in: Vector, out: Vector) => List(Coor(0))
      case (in: Matrix, out: Matrix) => keys.map(k => Coor(k.asInstanceOf[Coor2D].x, 0))
    }
  }.toList.distinct

  def isTotal() = {
    keys.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}

case class LinComBackwardQueryRule(inSpace: SubSpace, outSpace: SubSpace, keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = {
    (inSpace, outSpace) match {
      case (in: Vector, out: Vector) => List(Coor(0))
      case (in: Matrix, out: Matrix) => keys.map(k => Coor(k.asInstanceOf[Coor2D].x, 0))
    }
  }.toList.distinct

  def isTotal() = {
    keys.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}

case class GeoForwardQueryRule(inSpace: SubSpace, outSpace: SubSpace, tupleList: List[(Shape, Shape)], keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = keys.distinct

  def isTotal() = {
    keys.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}

case class GeoBackwardQueryRule(inSpace: SubSpace, outSpace: SubSpace, tupleList: List[(Shape, Shape)], keys: List[Coor]) extends QueryRule{
  lazy val reducedKeys = keys.distinct

  def isTotal() = {
    keys.size == outSpace.expand().size
  }

  def reduce = reducedKeys
}