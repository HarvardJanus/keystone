package workflow

import scala.math._

abstract class Shape(c: (Double, Double)) extends Serializable{
	def getCenter(): (Double, Double)
	def toCoor(): List[_]
	def inShape(i: Double, j: Double): Boolean
}

class Circle(c: (Double, Double), r: Double) extends Shape(c){
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-r).toInt to (x+r).toInt
			j <- (y-r).toInt to (y+r).toInt
			if (i-x)*(i-x)+(j-y)*(j-y) <= r*r
		} yield (i, j)
		l.toList
	}

	def inShape(i: Double, j: Double): Boolean = {
		val x = c._1
		val y = c._2
		if ((i-x)*(i-x)+(j-y)*(j-y) <= r*r) true else false
	}

	override def toString() = "center: "+c+" r: "+r
	def getCenter() = c
	def getR() = r
}

case class Ellipse(c: (Double, Double), a: Double, b: Double, theta: Double) extends Shape(c){
	def getCenter() = c
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-a).toInt to (x+a).toInt
			j <- (y-a).toInt to (y+a).toInt
			if inShape(i, j)
		} yield (i, j)
		l.toList
	}

	def inShape(i: Double, j: Double): Boolean = {
		val x = c._1
		val y = c._2
		val firstItem = ((i-x)*cos(theta) + (j-y)*sin(theta))/a
		val secondItem = ((j-y)*cos(theta) - (i-x)*sin(theta))/b
		if(firstItem*firstItem + secondItem*secondItem <= 1) true else false
	}

	override def toString() = "center: "+c+" major: "+a+" minor: "+b+" theta: "+theta
}

case class Square(c: (Double, Double), a: Double, b:Double) extends Shape(c){
	def getCenter() = c
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-a).toInt to (x+a).toInt
			j <- (y-b).toInt to (y+b).toInt
		} yield (i, j)
		l.toList
	}

	def inShape(i: Double, j: Double): Boolean = {
		val x = c._1
		val y = c._2
		if(i>=(x-a) && i<=(x+a) && j>=(y-b) && j<=(y+b)) true else false
	}

	override def toString() = "center: "+c+" width: "+2*a+" height: "+2*b
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

object Shape{
	def apply(input: List[(Int, Int)]): Shape = {
		detect(input)
	}

	def euclideanDistance(x: (Double, Double), y: (Double, Double)) = {
		(x._1-y._1)*(x._1-y._1) + (x._2-y._2)*(x._2-y._2)
	}

	def detect(input: List[(Int, Int)]): Shape = {
		val x = input.map(x => x._1.toDouble)
		val y = input.map(x => x._2.toDouble)
		val xCentroid = x.sum/x.length
		val yCentroid = y.sum/y.length
		val centroid = (xCentroid, yCentroid)

		val xLeft = x.min
		val xRight = x.max
		val yUp = y.min
		val yDown = y.max
		val square = Square((xLeft, yUp), (xRight, yDown))

		val furthest = {
			var distance = 0.0
			var point = (0.0, 0.0)
			for(p <- x.zip(y)){
				val dist = euclideanDistance(p, centroid)
				if ( dist > distance){
					distance = dist
					point = p
				}
			}
			point
		}

		val r = sqrt(euclideanDistance(furthest, centroid))
		val circle = Circle(centroid, r)

		val theta = if(((furthest._1 <= centroid._1)&&(furthest._2 <= centroid._2))||((furthest._1 > centroid._1)&&(furthest._2 > centroid._2))) asin(-centroid._1/r) else asin (centroid._1/r)
		//val theta = asin(-centroid._1/r)
		println("centroid: "+centroid+", furthest: "+furthest+", r: "+r+", theta: "+theta)

		val numerator = pow(r*((furthest._2-centroid._2)*cos(theta)-(furthest._1-centroid._1)*sin(theta)), 2)
		val denominator = pow(r, 2)-pow(((furthest._1-centroid._1)*cos(theta)+(furthest._2-centroid._2)*sin(theta)), 2)
		val b = sqrt(numerator/denominator)

		val ellipse = Ellipse(centroid, r, b, theta)

		println(square)
		println(circle)
		println(ellipse)

		println(square.toCoor)
		println(circle.toCoor)
		println(ellipse.toCoor)

		return square
	}
}
object Square{
	def apply(upperLeft: (Double, Double), lowerRight: (Double, Double)): Shape = {
		val c = ((upperLeft._1 + lowerRight._1)/2, (upperLeft._2 + lowerRight._2)/2)
		val a = abs(upperLeft._2 - lowerRight._2)/2
		val b = abs(upperLeft._1 - lowerRight._1)/2
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

		val list = List((0, 0), (0, 1), (1, 0), (1, 1))
		val shape = Shape(list)
		println(shape.toCoor)
  }
}