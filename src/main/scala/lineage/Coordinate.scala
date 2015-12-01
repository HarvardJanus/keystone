package lineage

abstract class Coor extends Serializable

case class Coor1D(x: Int) extends Coor {
  require(x>=0, {"coordinate can not be negative"})
  override def toString = x.toString
}

case class Coor2D(x:Int, y:Int) extends Coor {
  require((x>=0)&&(y>=0), {"coordinate can not be negative"})
  override def toString = "("+x+","+y+")"
}

case class Coor3D(x:Int, y:Int, c:Int) extends Coor {
  require((x>=0)&&(y>=0)&&(c>=0), {"coordinate can not be negative"})
  override def toString = "("+x+","+y+","+c+")"
}

case class CoorNull() extends Coor{
  override def toString = "null"
}

object Coor{
  def apply(x:Int): Coor = new Coor1D(x)
  def apply(x:Int, y:Int): Coor = new Coor2D(x, y)
  def apply(x:Int, y:Int, c:Int): Coor = new Coor3D(x, y, c)
  def apply() = new CoorNull()
}