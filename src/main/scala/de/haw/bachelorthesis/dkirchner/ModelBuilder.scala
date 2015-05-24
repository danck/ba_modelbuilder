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
      .filter(_.length > 15)
      .map(_.toLowerCase)
      .map(_.split(" ").toSeq) //Sonderzeichen rausnehmen
    documents.cache()

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf.cache()

    val relevanceVectorsRDD = tfidf.take(docWindowSize)
//    relevanceVectorsRDD.take(100).foreach(vector =>
//      println("Before: Value for \"Spark\" " + vector.apply(hashingTF.indexOf("Spark".toLowerCase)))
//    )

    val relevanceVectors = tfidf
      .take(docWindowSize)
      .reduce((vector1, vector2) =>
        mergeSparseVectors(vector1.asInstanceOf[SparseVector], vector2.asInstanceOf[SparseVector])
      )

    // (2) write the instance out to a file
    val oos = new ObjectOutputStream(new FileOutputStream("/tmp/tfidf"))
    oos.writeObject(relevanceVectors)
    oos.close()
    // (3) read the object back in
    val ois = new ObjectInputStream(new FileInputStream("/tmp/tfidf"))
    val model = ois.readObject.asInstanceOf[Seq]
    ois.close()
    // (4) print the object that was read back in

    println(model.toString())
    println(
        "Value for \"Spark\" " + model.apply(hashingTF.indexOf("Spark".toLowerCase))
          + "\nSize: " + model.size
          + "\nSize Indices: " + model.asInstanceOf[SparseVector].getIndices.length
          + "\nSize Values: " + model.asInstanceOf[SparseVector].getValues.length
    )
    println("Success " + Calendar.getInstance().getTime)
  }

  /**
   * Extends the class Spark SparseVector implementation by attribute getters.
   *
   * This is a helper class for mergeSparseVectors
   * @param sv
   */
  implicit class UnifiableSparseVector(sv: SparseVector) {
    def unifiableSparseVector: (Int, Array[Int], Array[Double]) = {
      (sv.size, sv.indices, sv.values)
    }

    def getIndices = sv.indices
    def getValues = sv.values
  }

  /**
   * Unifies two sparse vectors of the same length by adding their values at
   * each index
   * @param sv1
   * @param sv2
   * @return Vector that is the union of sv1 and sv2
   */
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