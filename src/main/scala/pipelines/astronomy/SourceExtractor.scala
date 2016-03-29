package pipelines.astronomy

import breeze.linalg._
import loaders.FitsLoader
import nodes.util.MaxClassifier
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import pipelines.Logging
import workflow._
import lineage._
import scopt.OptionParser

object RMS extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {
  def apply(in: DenseMatrix[Double]):DenseMatrix[Double] = {
    var matrix = Array.ofDim[Double](in.rows, in.cols)
    for(i <- 0 until in.rows; j <- 0 until in.cols)
      matrix(i)(j) = in(i,j)

    var bkg = new Background(matrix)

    new DenseMatrix(1, 1, Array(bkg.bkgmap.globalrms.toDouble))
  }

  override def saveLineageAndApply(in: RDD[DenseMatrix[Double]], tag: String): RDD[DenseMatrix[Double]] = {
    val out = in.map(apply)
    out.cache()
    val lineage = AllLineage(in, out, this)
    //lineage.save(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping size: "+lineage.qBackward(0,0,0).size)
    out
  }
}

object BkgSubstract extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {
  def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    var matrix = Array.ofDim[Double](in.rows, in.cols)
    for(i <- 0 until in.rows; j <- 0 until in.cols)
      matrix(i)(j) = in(i,j)

    var bkg = new Background(matrix)
    println("rms: "+bkg.bkgmap.globalrms)
    val newMatrix = bkg.subfrom(matrix)

    var stream = Array.ofDim[Double](in.rows * in.cols)
    (0 until in.rows).map(i => {
      Array.copy(newMatrix(i), 0, stream, i*in.cols, in.cols)
    })
    new DenseMatrix(in.cols, in.rows, stream).t
  }

  override def saveLineageAndApply(in: RDD[DenseMatrix[Double]], tag: String): RDD[DenseMatrix[Double]] = {
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val lineage = IdentityLineage(in, out, this)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    lineage.saveOutput(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping size: "+lineage.qBackward(0,0,0).size)
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}

class RMSEstimator extends Estimator[DenseMatrix[Double], DenseMatrix[Double]] with Logging{
  def fit(samples: RDD[DenseMatrix[Double]]): ExtractTransformer = {
    val sampleList = samples.map(x=>x(0,0)).collect
    val rms = DenseVector(sampleList.sum/sampleList.size)
    new ExtractTransformer(rms)
  }
}

case class ExtractTransformer(rmsVector: DenseVector[Double]) extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {
  def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    /*hard-coded rms results in more objects detected, this is temporary
     *solution to make intermediate data solely as dense matrix
     */
    val rms = rmsVector(0)
    val ex = new Extractor
    var matrix = Array.ofDim[Double](in.rows, in.cols)
    for(i <- 0 until in.rows; j <- 0 until in.cols)
      matrix(i)(j) = in(i,j)

    val objects = ex.extract(matrix, (1.5 * rms).toFloat)
    val array = objects.map(_.toDoubleArray)
    //array.map(o=>println("a: "+o(12)+", b: "+o(13)+", theta:"+o(14)))
    val rows = objects.size
    val cols = 28
    var stream = Array.ofDim[Double](rows*cols)
    (0 until rows).map(i => {
      Array.copy(array(i), 0, stream, i*cols, cols)
    })
    new DenseMatrix(cols, rows, stream).t
  }

  override def saveLineageAndApply(in: RDD[DenseMatrix[Double]], tag: String): RDD[DenseMatrix[Double]] = {
    val stamp1 = System.nanoTime()
    in.cache()
    val outRDD = in.map(m=>{
      val rms = rmsVector(0)
      val ex = new Extractor
      var matrix = Array.ofDim[Double](m.rows, m.cols)
      for(i <- 0 until m.rows; j <- 0 until m.cols)
        matrix(i)(j) = m(i,j)

      val objects = ex.extract(matrix, (1.5 * rms).toFloat)
      val array = objects.map(_.toDoubleArray)

      val ioList = array.zipWithIndex.map{
        case (row, index) => {
          val ellipse = Shape((row(7), row(8)), row(12), row(13), row(14))
          val square = Shape((index.toDouble, 0.0), (index.toDouble, row.size.toDouble))
          (ellipse, square)
        }
      }.toList     

      val rows = objects.size
      val cols = 28
      var stream = Array.ofDim[Double](rows*cols)
      (0 until rows).map(i => {
        Array.copy(array(i), 0, stream, i*cols, cols)
      })
      (new DenseMatrix(cols, rows, stream).t, ioList)
    })
    
    val out = outRDD.map(_._1)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val ioList = outRDD.map(_._2)
    ioList.cache()

    val lineage = GeoLineage(in, out, ioList, this)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    lineage.saveOutput(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward((0,0,0)))
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}

object Counter extends Transformer[DenseMatrix[Double], Int] {
  def apply(in: DenseMatrix[Double]): Int = in.rows
}

object SourceExtractor extends Serializable with Logging {
  val appName = "SourceExtractor"

  def run(sc: SparkContext, conf: SourceExtractorConfig) {
    val startTime = System.nanoTime()
    
    val dataset = FitsLoader(sc, conf.inputPath)
    //val dataset = data.coalesce(500)
    dataset.cache

    //val extractor = (new RMSEstimator).fit(RMS(dataset))
    val extractor = new ExtractTransformer(DenseVector[Double](4.71))
    val pipeline = BkgSubstract andThen extractor andThen Counter

    val count = pipeline(dataset).reduce(_+_)
    logInfo(s"Detected ${count} objects")
    val endTime = System.nanoTime()
    logInfo(s"Pipeline took ${(endTime - startTime)/1e9} s")
  }

  case class SourceExtractorConfig(
      inputPath: String = ""
  )

  def parse(args: Array[String]): SourceExtractorConfig = new OptionParser[SourceExtractorConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("inputPath") required() action { (x,c) => c.copy(inputPath=x) }
  }.parse(args, SourceExtractorConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[1]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
