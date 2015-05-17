package de.haw.bachelorthesis.dkirchner

/**
 *
 * Created by Daniel on 12.05.2015.
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.util.Utils


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

    val documents: RDD[Seq[String]] = sc.textFile(textFile)
      .filter(_.size > 15)
      .map(_.toLowerCase())
      .map(_.split(" ").toSeq)
    documents.cache()

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf.cache()

    //tfidf.foreach(elem => println(elem))
    /*println("---------DOCUMENTS----------")
    documents.take(10).foreach(println(_))
    println("---------TF----------")
    tf.foreach(println(_))
    println("---------IDF----------")
    println(idf.toString)
    println("---------TF-IDF----------")
    tfidf.take(10).foreach((println(_)))*/

    println("########## tf count: " + tf.count())
    println("########## index of Spark in TF: " + hashingTF.indexOf("Spark"))
    println("########## first 100: ")
    tfidf.take(100).foreach(vector => println(vector.apply(hashingTF.indexOf("Spark".toLowerCase))))

    //Vectors.sparse(12, Array(1,2,3), Array(0.0, 0.1, 0.2)).apply(1)


    println("SUCCESS 11.0")
  }
}