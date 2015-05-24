package de.haw.bachelorthesis.dkirchner

/**
 *
 * Created by Daniel on 12.05.2015.
 */

import java.io.{FileInputStream, ObjectInputStream, FileOutputStream, ObjectOutputStream}
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector}
import org.apache.spark.util.Utils


/**
 *
 */
object ModelBuilder {
  val docWindowSize: Integer = 500

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
      .map(_.split(" ").toSeq) //Sonderzeichen rausnehmen
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

    Vectors.sparse(12, Array(1,2,3), Array(0.0, 0.1, 0.2))

    val relevanceVectors = tfidf.take(docWindowSize)
    relevanceVectors.take(100).foreach(vector =>
      println("Before: Value for \"Spark\" " + vector.apply(hashingTF.indexOf("Spark".toLowerCase)))
    )

    val relevanceVector = tfidf.take(docWindowSize) //.reduce((a, b) => mergeVectors(a,b))

    // (2) write the instance out to a file
    val oos = new ObjectOutputStream(new FileOutputStream("/tmp/tfidf"))
    oos.writeObject(relevanceVectors)
    oos.close
    // (3) read the object back in
    val ois = new ObjectInputStream(new FileInputStream("/tmp/tfidf"))
    val stock = ois.readObject.asInstanceOf[Array[Vector]]
    ois.close
    // (4) print the object that was read back in
    val reducedRelVec = stock.take(100)
      .reduce((elem1, elem2) =>
        mergeSparseVectors(elem1.asInstanceOf[SparseVector], elem2.asInstanceOf[SparseVector])
    )

    println(reducedRelVec.toString)
    println(
        "After: Value for \"Spark\" " + reducedRelVec.apply(hashingTF.indexOf("Spark".toLowerCase))
    )

    println("########## red vec size: " + tf.count())
    println("########## index of Spark in TF: " + hashingTF.indexOf("Spark"))

    println("Success " + Calendar.getInstance().getTime())
  }

  implicit class UnifiableSparseVector(sv: SparseVector) {
    def unifiableSparseVector: (Int, Array[Int], Array[Double]) = {
      (sv.size, sv.indices, sv.values)
    }

    def getIndices = sv.indices
    def getValues = sv.values
  }

  def mergeSparseVectors(sv1: SparseVector, sv2: SparseVector): Vector = {
    if (sv1.size != sv2.size)
      throw  new IllegalArgumentException("Input vectors must be of equal size")

    val indices1 = sv1.getIndices
    //val values1 = sv1.getValues
    val indices2 = sv2.getIndices
    //val values2 = sv2.getValues

    val indices = indices1.union(indices2)
    val values = indices.map(index => sv1.apply(index) + sv2.apply(index))

    if (indices.length != values.length)
      throw new IllegalArgumentException("Length of indices and values must be equal but is "
        + indices.length + "(Indices) " + values.length + "(Values)")

    val result = Vectors.sparse(sv1.size, indices, values)

    result
  }
}