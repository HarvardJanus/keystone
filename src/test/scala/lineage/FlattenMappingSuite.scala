package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class FlattenMappingSuite extends FunSuite with Logging {
  test("FlattenMapping Matrix2Vector Test"){
    val m = DenseMatrix.zeros[Double](5, 4)
    val v = DenseVector.zeros[Double](20)

    /*
     *  Flatten a matrix by breaking the yDim
     */
    val mapping1 = FlattenMapping(m, v, 1)
    assert(mapping1.qForward(List(Coor(3,2))).toString == "List(13)")
    assert(mapping1.qBackward(List(Coor(13))).toString == "List((3,2))")

    /*
     *  Flatten a matrix by breaking the xDim
     */
    val v2 = DenseVector.zeros[Double](5)
    val mapping2 = FlattenMapping(m, v, 0)
    assert(mapping2.qForward(List(Coor(3,2))).toString == "List(14)")
    assert(mapping2.qBackward(List(Coor(14))).toString == "List((3,2))")
  }

  test("FlattenMapping Seq[Vector]2Vector Test"){
    val v1 = DenseVector.zeros[Double](5)
    val s = Seq(v1, v1)
    val v2 = DenseVector.zeros[Double](10)

    val mapping = FlattenMapping(s, v2)
    assert(mapping.qForward(List(Coor(1,0))).toString == "List(5)")
    assert(mapping.qBackward(List(Coor(5))).toString == "List((1,0))")
  }

  test("FlattenMapping Vector2Matrix Test"){
    val m = DenseMatrix.zeros[Double](5, 4)
    val v = DenseVector.zeros[Double](20)

    /*
     *  Flatten a matrix by breaking the yDim
     */
    val mapping1 = FlattenMapping(v, m, 1)
    assert(mapping1.qForward(List(Coor(13))).toString == "List((3,2))")
    assert(mapping1.qBackward(List(Coor(3,2))).toString == "List(13)")

    /*
     *  Flatten a matrix by breaking the xDim
     */
    val v2 = DenseVector.zeros[Double](5)
    val mapping2 = FlattenMapping(v, m, 0)
    assert(mapping2.qForward(List(Coor(14))).toString == "List((3,2))")
    assert(mapping2.qBackward(List(Coor(3,2))).toString == "List(14)")
  }

  test("FlattenMapping Matrix2Vector Query Optimization Test"){
    val m = DenseMatrix.zeros[Double](10, 1000)
    val v = DenseVector.zeros[Double](10000)
    
    val fKeys = (for(i <- 0 until m.rows-1; j <- 0 until m.cols) yield Coor(i, j)).toList
    val bKeys = (for(i <- 0 until v.size-1) yield Coor(i)).toList

    val trials = 3
    val list1 = (0 until trials).map(x => {
      val mapping = FlattenMapping(m, v, 1)
      println(mapping.qForward(fKeys)(0))
      time(mapping.qForward(fKeys))
    }).toList
    
    val list2 = (0 until trials).map(x => {
      val mapping = FlattenMapping(m, v, 1)
      println(mapping.qBackward(bKeys)(0))
      time(mapping.qBackward(bKeys))
    }).toList

    Mapping.setOpzFlag(true)

    val list3 = (0 until trials).map(x => {
      val mapping = FlattenMapping(m, v, 1)
      println(mapping.qForward(fKeys)(0))
      time(mapping.qForward(fKeys))
    }).toList

    val list4 = (0 until trials).map(x => {
      val mapping = FlattenMapping(m, v, 1)
      println(mapping.qBackward(bKeys)(0))
      time(mapping.qBackward(bKeys))
    }).toList

    println("non-optimized forward: "+list1.sum/list1.size)
    println("optimized forward: "+list3.sum/list3.size)
    println("non-optimized backward: "+list2.sum/list2.size)
    println("optimized backward: "+list4.sum/list4.size)
    Mapping.setOpzFlag(false)
  }

  test("FlattenMapping Vector2Matrix Query Optimization Test"){
    val m = DenseMatrix.zeros[Double](10, 1000)
    val v = DenseVector.zeros[Double](10000)
    
    val fKeys = (for(i <- 0 until v.size-1) yield Coor(i)).toList
    val bKeys = (for(i <- 0 until m.rows-1; j <- 0 until m.cols) yield Coor(i, j)).toList

    val trials = 3
    val list1 = (0 until trials).map(x => {
      val mapping = FlattenMapping(v, m, 1)
      println(mapping.qForward(fKeys)(0))
      time(mapping.qForward(fKeys))
    }).toList
    
    val list2 = (0 until trials).map(x => {
      val mapping = FlattenMapping(v, m, 1)
      println(mapping.qBackward(bKeys)(0))
      time(mapping.qBackward(bKeys))
    }).toList

    Mapping.setOpzFlag(true)

    val list3 = (0 until trials).map(x => {
      val mapping = FlattenMapping(v, m, 1)
      println(mapping.qForward(fKeys)(0))
      time(mapping.qForward(fKeys))
    }).toList

    val list4 = (0 until trials).map(x => {
      val mapping = FlattenMapping(v, m, 1)
      println(mapping.qBackward(bKeys)(0))
      time(mapping.qBackward(bKeys))
    }).toList

    println("non-optimized forward: "+list1.sum/list1.size)
    println("optimized forward: "+list3.sum/list3.size)
    println("non-optimized backward: "+list2.sum/list2.size)
    println("optimized backward: "+list4.sum/list4.size)
    Mapping.setOpzFlag(false)
  }

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    (System.nanoTime-s)/1e9
  }
}