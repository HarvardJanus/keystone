package workflow

import scala.math._

trait Shape extends Serializable{
	def toCoor(): List[_]
}

class Circle(c: (Double, Double), r: Double) extends Shape{
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-r).toInt to (x+r).toInt
			j <- (y-r).toInt to (y+r).toInt
			if i*i+j*j <= r*r
		} yield (i, j)
		l.toList
	}

	override def toString() = "center: "+c+"\t r: "+r
}

case class Ellipse(c: (Double, Double), a: Double, b: Double, theta: Double) extends Shape{
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-a).toInt to (x+a).toInt
			j <- (y-a).toInt to (y+a).toInt
			if inEllipse(i, j)
		} yield (i, j)
		l.toList
	}

	def inEllipse(i: Double, j: Double): Boolean = {
		val firstItem = ((i-c._1)*cos(theta) + (j-c._2)*sin(theta))/a
		val secondItem = ((i-c._1)*sin(theta) + (j-c._2)*cos(theta))/b
		if(firstItem*firstItem + secondItem*secondItem <= 1)
			return true
		else
			return false
	}

	override def toString() = "center: "+c+"\t major: "+a+"\t minor: "+b+"\t theta: "+theta
}

case class Square(c: (Double, Double), a: Double, b:Double) extends Shape{
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-a).toInt to (x+a).toInt
			j <- (y-a).toInt to (y+a).toInt
		} yield (i, j)
		l.toList
	}

	override def toString() = "center: "+c+"\t width: "+2*a+"\t height: "+2*b
}

object Circle{
	def apply(c: (Double, Double), r: Double): Shape = {
		new Circle(c, r)
	}
}

/*object Ellipse{
	override def apply(c: (Double, Double), a: Double, b: Double, theta: Double): Shape = {
		require((a >= b), {"ellipse major has to be greater than or equal to minor"})
		new Ellipse(c, a, b, theta)
	}
}*/

object Square{
	def apply(lowerLeft: (Double, Double), upperRight: (Double, Double)): Shape = {
		val c = ((lowerLeft._1 + upperRight._1)/2, (lowerLeft._2 + upperRight._2)/2)
		val a = abs(lowerLeft._1 - upperRight._1)/2
		val b = abs(lowerLeft._2 - upperRight._2)/2
		new Square(c, a, b)
	}
}
object ShapeTester{
	def main(args: Array[String]) {
    val circle = Circle((0, 0), 2.0)
    println(circle.toCoor)

    val ellipse = Ellipse((0, 0), 2, 1, 0)
    println(ellipse.toCoor)

    val square = Square((0, 0), 1, 1)
    println(square.toCoor)

    val square1 = Square((-1, -1), (1, 1))
		println(square1.toCoor)
  }
}