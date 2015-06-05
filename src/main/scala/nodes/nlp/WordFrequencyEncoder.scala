package nodes.nlp

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import workflow.{Estimator, Transformer}

object WordFrequencyEncoder extends Estimator[Seq[String], Seq[Int]] {
  private[this] def makeUnigrams(data: RDD[Seq[String]]) =
    NGramsCounts[String]().apply(NGramsFeaturizer[String](1 to 1).apply(data))

  // TODO: alternative approach: collectAsMap once, let driver do the work.
  def fit(data: RDD[Seq[String]]): WordFrequencyTransformer = {
    val unigramCounts = makeUnigrams(data)

    val wordIndex = unigramCounts
      .zipWithIndex() // indexes respect the sorted order
      .map { case ((unigram, count), index) =>
        // valid if # of word types in training data is less than Int.MaxValue
        (unigram.words(0), index.asInstanceOf[Int])
      }.collectAsMap()

    val wordIndexBroadcast = unigramCounts.sparkContext.broadcast(wordIndex)

    val unigrams = unigramCounts.map { case (unigram, count) =>
      (wordIndexBroadcast.value(unigram.words(0)), count)
    }.collectAsMap()

    new WordFrequencyTransformer(wordIndexBroadcast, unigrams)
  }

}

/**
 * Encodes string tokens as non-negative integers, which are indices of the
 * tokens' positions in the sorted-by-frequency order.  Out-of-vocabulary words
 * are mapped to the special index -1.
 *
 * The parameters passed to this class are usually calculated by [[WordFrequencyEncoder]].
 *
 * @param wordIndexBroadcast A mapping from token string to its frequency-ordered index
 * @param unigramCounts the counts of unigrams in the training corpus
 */
class WordFrequencyTransformer(
    wordIndexBroadcast: Broadcast[scala.collection.Map[String, Int]],
    val unigramCounts: scala.collection.Map[Int, Int])
  extends Transformer[Seq[String], Seq[Int]] {

  final val OOV_INDEX = -1

  override def apply(in: RDD[Seq[String]]): RDD[Seq[Int]] = {
    in.mapPartitions { case part =>
      val index = wordIndexBroadcast.value
      part.map(ngram => ngram.map(index.getOrElse(_, OOV_INDEX)))
    }
  }

  def apply(words: Seq[String]): Seq[Int] = {
    val index = wordIndexBroadcast.value
    words.map(index.getOrElse(_, OOV_INDEX))
  }

}
