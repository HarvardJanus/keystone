package pipelines.astronomy

import loaders.FitsLoader
import nodes.util.MaxClassifier
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import pipelines.Logging
import workflow._
import scopt.OptionParser

object BkgSubstract extends Transformer[Array[Array[Double]], (Array[Array[Double]], Double)] {
  def apply(in: Array[Array[Double]]): (Array[Array[Double]], Double) = {
    var bkg = new Background(in)
    val newMatrix = bkg.subfrom(in)
    return (newMatrix, bkg.bkgmap.globalrms)
  }
}

object Extract extends Transformer[(Array[Array[Double]], Double), Array[Sepobj]] {
  def apply(in: (Array[Array[Double]], Double)): Array[Sepobj] = {
    val ex = new Extractor
    val (matrix, rms) = in
    val objects = ex.extract(matrix, (1.5 * rms).toFloat)
    return objects
  }
}

object Counter extends Transformer[Array[Sepobj], Int] {
  def apply(in: Array[Sepobj]): Int = in.size
}

object SourceExtractor extends Serializable with Logging {
  val appName = "SourceExtractor"

  def run(sc: SparkContext, conf: SourceExtractorConfig) {
    val pipeline = BkgSubstract andThen Extract andThen Counter
    val startTime = System.nanoTime()
    val count = pipeline(FitsLoader(sc, conf.inputPath)).reduce(_ + _)
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
