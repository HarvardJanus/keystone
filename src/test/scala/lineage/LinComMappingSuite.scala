package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class LinComMappingSuite extends FunSuite with Logging {
  test("LinComMapping Vector Test"){
    val v1 = DenseVector.zeros[Double](4)
    val v2 = DenseVector.zeros[Double](4)
    val mapping = LinComMapping(v1, v2)
    assert(mapping.qForward(List(Coor(0))).toString == "List(0, 1, 2, 3)")
    assert(mapping.qBackward(List(Coor(0))).toString == "List(0, 1, 2, 3)")
  }

  test("LinComMapping Matrix Test"){
    val m1 = DenseMatrix.zeros[Double](5, 4)
    val m2 = DenseMatrix.zeros[Double](5, 2)
    val mapping = LinComMapping(m1, m2)
    assert(mapping.qForward(List(Coor(2,0))) == List(Coor(2,0), Coor(2,1)))
    assert(mapping.qBackward(List(Coor(2,0))) == List(Coor(2,0), Coor(2,1), Coor(2,2), Coor(2,3)))
  }

  test("LinComMapping Vector2Vector Query Optimization Test"){
    val v1 = DenseVector.zeros[Double](5)
    val v2 = DenseVector.zeros[Double](5)
    
    val fKeys = (for(i <- 0 until v1.size-1) yield Coor(i)).toList
    val bKeys = (for(i <- 0 until v2.size-1) yield Coor(i)).toList

    val trials = 3
    val list1 = (0 until trials).map(x => {
      val mapping = LinComMapping(v1, v2)
      //println(mapping.qForward(fKeys))
      time(mapping.qForward(fKeys))
    }).toList
    
    val list2 = (0 until trials).map(x => {
      val mapping = LinComMapping(v1, v2)
      //println(mapping.qBackward(bKeys))
      time(mapping.qBackward(bKeys))
    }).toList

    Mapping.setOpzFlag(true)

    val list3 = (0 until trials).map(x => {
      val mapping = LinComMapping(v1, v2)
      //println(mapping.qForward(fKeys))
      time(mapping.qForward(fKeys))
    }).toList

    val list4 = (0 until trials).map(x => {
      val mapping = LinComMapping(v1, v2)
      //println(mapping.qBackward(bKeys))
      time(mapping.qBackward(bKeys))
    }).toList

    println("non-optimized forward: "+list1.sum/list1.size)
    println("optimized forward: "+list3.sum/list3.size)
    println("non-optimized backward: "+list2.sum/list2.size)
    println("optimized backward: "+list4.sum/list4.size)
    Mapping.setOpzFlag(false)
  }

  test("LinComMapping Matrix2Matrix Query Optimization Test"){
    val m1 = DenseMatrix.zeros[Double](5, 3)
    val m2 = DenseMatrix.zeros[Double](5, 2)
    
    val fKeys = (for(i <- 0 until m1.rows; j <- 0 until m1.cols-1) yield Coor(i,j)).toList
    val bKeys = (for(i <- 0 until m2.rows; j <- 0 until m2.cols-1) yield Coor(i,j)).toList

    val trials = 3
    val list1 = (0 until trials).map(x => {
      val mapping = LinComMapping(m1, m2)
      //println(mapping.qForward(fKeys))
      time(mapping.qForward(fKeys))
    }).toList
    
    val list2 = (0 until trials).map(x => {
      val mapping = LinComMapping(m1, m2)
      //println(mapping.qBackward(bKeys))
      time(mapping.qBackward(bKeys))
    }).toList

    Mapping.setOpzFlag(true)

    val list3 = (0 until trials).map(x => {
      val mapping = LinComMapping(m1, m2)
      //println(mapping.qForward(fKeys))
      time(mapping.qForward(fKeys))
    }).toList

    val list4 = (0 until trials).map(x => {
      val mapping = LinComMapping(m1, m2)
      //println(mapping.qBackward(bKeys))
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