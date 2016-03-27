package lineage

trait QueryRule{
  def isTotal(): Boolean
  def reduce(): List[Coor]
}

case class CollapseQueryRule(inSpace: SubSpace, outSpace: SubSpace, dim: Int, keys: List[Coor]) extends QueryRule{
  val reducedKeys = {
    (inSpace, outSpace) match {
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
    keys.distinct.size == inSpace.expand().size
  }

  def reduce() = reducedKeys
}

case class FlattenQueryRule(inSpace: SubSpace, outSpace: SubSpace, dim: Int, keys: List[Coor]) extends QueryRule{
  val reducedKeys = keys.distinct

  def isTotal() = {
    reducedKeys.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}

case class IdentityQueryRule(inSpace: SubSpace, outSpace: SubSpace, keys: List[Coor]) extends QueryRule{
  val reducedKeys = keys.distinct

  def isTotal() = {
    reducedKeys.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}

case class AllQueryRule(inSpace: SubSpace, outSpace: SubSpace, keys: List[Coor]) extends QueryRule{
  val reducedKeys = keys.distinct

  def isTotal() = true

  def reduce = reducedKeys
}

case class LinComQueryRule(inSpace: SubSpace, outSpace: SubSpace, keys: List[Coor]) extends QueryRule{
  val reducedKeys = {
    (inSpace, outSpace) match {
      case (in: Vector, out: Vector) => keys
      case (in: Matrix, out: Matrix) => keys.map(k => Coor(k.asInstanceOf[Coor2D].x, 0))
    }
  }.toList.distinct

  def isTotal() = {
    keys.distinct.size == inSpace.expand().size
  }

  def reduce = reducedKeys
}