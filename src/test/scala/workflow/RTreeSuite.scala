package workflow

import archery._
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class RTreeSuite extends FunSuite with LocalSparkContext with Logging {
	/**
	 *	RTree requires the Box to be defined as (lowerLeftX, lowerLeftY, upperRightX, upperRightY)
	 */
	test("RTree test") {
		val c1 = Entry(Box(0F, 0F, 1F, 1F), "alice")
		val c2 = Entry(Box(0.4F, 0F, 1.4F, 1F), "bob")
		val tree: RTree[String] = RTree(c1, c2)
		val result = tree.searchWithIn(Point(0.5F, 0.5F))
		assert(result.size == 2)
	}
}