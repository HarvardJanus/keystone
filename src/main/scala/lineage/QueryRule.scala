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
          case 0 => Coor(k.asInstanceOf[Coor2D].y)
          case 1 => Coor(k.asInstanceOf[Coor2D].x)
        }
      })
      case (in: Image, out: Matrix) => keys.map( k => {
        dim match {
          case 0 => Coor(k.asInstanceOf[Coor3D].y, k.asInstanceOf[Coor3D].c)
          case 1 => Coor(k.asInstanceOf[Coor3D].x, k.asInstanceOf[Coor3D].c)
          case 2 => Coor(k.asInstanceOf[Coor3D].x, k.asInstanceOf[Coor3D].y)
        }
      })
    }
  }.toList.distinct

  def isTotal() = {
    reducedKeys.size == outSpace.expand().size
  }

  def reduce() = reducedKeys
}

object CollpaseQueryRule{
  def apply(inSpace: SubSpace, outSpace: SubSpace, dim: Int, keys: List[Coor]) = 
    new CollapseQueryRule(inSpace: SubSpace, outSpace: SubSpace, dim: Int, keys: List[Coor])
}
