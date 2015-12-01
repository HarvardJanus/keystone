package lineage

trait Mapping extends Serializable{
  def qForward(keys: List[Coor]): List[Coor]
  def qBackward(keys: List[Coor]): List[Coor]
}

case class IdentityMapping(inSpace: SubSpace, outSpace: SubSpace) extends Mapping{
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    flag match {
      case true => keys
      case false => List(Coor())
    }
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    flag match {
      case true => keys
      case false => List(Coor())
    }
  }
}