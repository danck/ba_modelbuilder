package de.haw.bachelorthesis.dkirchner

/**
 *
 * Created by Daniel on 12.05.2015.
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vector

/**
 *
 */
object ModelBuilder {
  def main (args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ModelBuilder <master> <textfile>")
      System.exit(1)
    }

    val Array(master, textFile) = args.take(2)
    val sparkConf = new SparkConf().setAppName("Model Builder").setMaster(master)
    val sc = new SparkContext(sparkConf)

    val documents: RDD[Seq[String]] = sc.textFile(textFile).map(_.split("-------------------------").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    tfidf.foreach(elem => println(elem))

    println("SUCCESS")
  }
}