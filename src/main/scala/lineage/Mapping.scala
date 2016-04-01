package lineage

abstract class Mapping(inSpace: SubSpace, outSpace: SubSpace) extends Serializable{
  def qForward(keys: List[Coor]): List[Coor]
  def qBackward(keys: List[Coor]): List[Coor]
  def getInSpace() = inSpace
  def getOutSpace() = outSpace
}

object Mapping {
  var queryOptimization = true
  def setOpzFlag(v: Boolean) = {
    queryOptimization = v
  }
}