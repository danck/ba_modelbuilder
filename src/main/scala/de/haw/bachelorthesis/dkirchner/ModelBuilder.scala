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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

/**
 *
 */
object ModelBuilder {
  def main (args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: ModelBuilder <textfile>")
      System.exit(1)
    }

    val Array(textFile) = args.take(1)
    val sparkConf = new SparkConf().setAppName("Model Builder")
    val sc = new SparkContext(sparkConf)

    val conf = new Configuration(sc.hadoopConfiguration)
    conf.set("textinputformat.record.delimiter", "\n")
    val input = sc.newAPIHadoopFile(textFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)

    //val documents: RDD[Seq[String]] = sc.textFile(textFile).
    val documents: RDD[Seq[String]] = input
      .map {case (_, text) => text.toString.split(" ").toSeq }
    documents.cache()

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    //tfidf.foreach(elem => println(elem))
    documents.take(100).foreach(println(_))

    println("SUCCESS 7.0")
  }
}