package lineage

import archery._
import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging

class GeoMappingSuite extends FunSuite with Logging {
  test("Geo Matrix-to-Matrix Mapping test"){
    val m1 = DenseMatrix.zeros[Double](5,8)
    val m2 = DenseMatrix.zeros[Double](5,5)
    val c1 = Shape((2.0, 2.0), 2.0)
    val c2 = Shape((2.0, 5.0), 2.0)
    val s1 = Shape((1.0, 2.0), 1.0, 1.0)
    val s2 = Shape((1.0, 4.0), 1.0, 1.0)
    val l = List((c1, s1), (c2, s2))
    val mapping = GeoMapping(m1, m2, l)
      
    val fResult = mapping.qForward(List(Coor(2,2)))
    assert(fResult.toString == "List((0,1), (0,2), (0,3), (1,1), (1,2), (1,3), (2,1), (2,2), (2,3))")

    val fResult1 = mapping.qForward(List(Coor(0,0)))
    assert(fResult1.toString == "List()")

    val bResult = mapping.qBackward(List(Coor(1, 2)))
    assert(bResult.toString == "List((0,2), (1,1), (1,2), (1,3), (2,0), (2,1), (2,2), (2,3), (2,4), (3,1), (3,2), (3,3), (4,2))")
  }

  test("GeoMapping Matrix-to-Matrix Query Optimization test"){
    val m1 = DenseMatrix.zeros[Double](5,8)
    val m2 = DenseMatrix.zeros[Double](5,5)
    val c1 = Shape((2.0, 2.0), 2.0)
    val c2 = Shape((2.0, 5.0), 2.0)
    val s1 = Shape((1.0, 2.0), 1.0, 1.0)
    val s2 = Shape((1.0, 4.0), 1.0, 1.0)
    val l = List((c1, s1), (c2, s2))
    val mapping = GeoMapping(m1, m2, l)

    val fKeys = (for(i <- 0 until m1.rows; j <- 0 until m1.cols-1) yield Coor(i,j)).toList
    val bKeys = (for(i <- 0 until m2.rows; j <- 0 until m2.cols-1) yield Coor(i,j)).toList

    val trials = 3
    val list1 = (0 until trials).map(x => {
      val mapping = GeoMapping(m1, m2, l)
      //println(mapping.qForward(fKeys).size)
      time(mapping.qForward(fKeys))
    }).toList
    
    val list2 = (0 until trials).map(x => {
      val mapping = GeoMapping(m1, m2, l)
      //println(mapping.qBackward(bKeys).size)
      time(mapping.qBackward(bKeys))
    }).toList

    Mapping.setOpzFlag(true)

    val list3 = (0 until trials).map(x => {
      val mapping = GeoMapping(m1, m2, l)
      //println(mapping.qForward(fKeys).size)
      time(mapping.qForward(fKeys))
    }).toList

    val list4 = (0 until trials).map(x => {
      val mapping = GeoMapping(m1, m2, l)
      //println(mapping.qBackward(bKeys).size)
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