package loaders

import java.lang.{ Double => jDouble }
import java.nio.ByteBuffer
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.eso.fits._

/**
 * Data Loader that loads csv files of comma separated numbers into an RDD of DenseVectors
 */
object FitsLoader {
  /**
   * Load FITS files from the given path into an RDD of HDULists
   * @param sc The spark context to use
   * @param path The path to the FITS files
   * @return RDD of HDULists, one per FITS file
   */
  def apply(sc: SparkContext, path: String): RDD[Array[Array[Double]]] = {
    val flist = sc.binaryFiles(path)
    flist.map{ content =>
      val is = new ByteArrayInputStream(content._2.toArray)
      val dis = new DataInputStream(is)
      val file = new FitsFile(dis, true)
      val hdu: FitsHDUnit = file.getHDUnit(0)
      val dm: FitsMatrix = hdu.getData().asInstanceOf[FitsMatrix]
      val naxis: Array[Int] = dm.getNaxis()
      val ncol = naxis(0)
      val nval = dm.getNoValues()
      val nrow = nval / ncol
      println("nrow: " + nrow + "\t ncol:" + ncol)
      // build and populate an array
      var matrix = Array.ofDim[Float](nrow, ncol)
      (0 until nrow).map(i => dm.getFloatValues(i * ncol, ncol, matrix(i)))

      println("matrix: height: " + nrow + "\twidth: " + ncol)

      var rmatrix = Array.ofDim[Double](nrow, ncol)
      for (i <- (0 until nrow)) {
        for (j <- (0 until ncol)) {
          rmatrix(i)(j) = matrix(i)(j).toDouble
        }
      }
      rmatrix
    }
  }
}