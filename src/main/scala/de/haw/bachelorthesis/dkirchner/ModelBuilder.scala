package de.haw.bachelorthesis.dkirchner

/**
 *
 * Created by Daniel on 12.05.2015.
 */

import java.io._
import java.util.Calendar

import javax.mail._
import javax.mail.internet._
import javax.mail.search._
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector}


/**
 *
 */
object ModelBuilder {
  val docWindowSize: Integer = 1500

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

    val hashingTF = new HashingTF(1 << 20)
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()

    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val relevanceVector = tfidf
      .take(docWindowSize)
      .reduce((vector1, vector2) =>
        mergeSparseVectors(vector1.asInstanceOf[SparseVector], vector2.asInstanceOf[SparseVector])
      )

    // (2) write the model instance out to a file
    try {
      val oos = new ObjectOutputStream(new FileOutputStream("/tmp/tfidf"))
      oos.writeObject(relevanceVector)
      oos.close()
    } catch {
      case e: Exception => println("Exception while saving model: " + e)
    }

    checkMail()

    println("FINISHED " + Calendar.getInstance().getTime)
  }

  /**
   * Extends the class Spark SparseVector implementation by attribute getters.
   * This is a helper class for the local method mergeSparseVectors
   * @param sv Regular SparseVector to be extended
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
   * @param sv1 unifiableSparseVector
   * @param sv2 unifiableSparseVector
   * @return Vector that is the union of sv1 and sv2
   */
  def mergeSparseVectors(sv1: SparseVector, sv2: SparseVector): Vector = {
    if (sv1.size != sv2.size)
      throw  new IllegalArgumentException("Input vectors must be of equal size")

    val indices1 = sv1.getIndices
    val indices2 = sv2.getIndices

    val indices = indices1.union(indices2).distinct.sorted
    val values = indices.map(index => sv1.apply(index) + sv2.apply(index))

    val result = Vectors.sparse(sv1.size, indices, values)

    result
  }

  def checkMail(): Unit = {
    val props = System.getProperties()
    props.setProperty("mail.store.protocol", "imaps")
    val session = Session.getDefaultInstance(props, null)
    val store = session.getStore("imaps")
    try {
      // use imap.gmail.com for gmail
      store.connect("imap.gmail.com", "danomonitoring@googlemail.com", "monitoring4me!")
      val inbox = store.getFolder("Inbox")
      inbox.open(Folder.READ_ONLY)

      // limit this to 20 message during testing
      val messages = inbox.getMessages()
      val limit = 20
      var count = 0
      for (message <- messages) {
        count = count + 1
        if (count > limit) System.exit(0)
        println(message.getSubject())
      }
      inbox.close(true)
    } catch {
      case e: NoSuchProviderException =>  e.printStackTrace()
        System.exit(1)
      case me: MessagingException =>      me.printStackTrace()
        System.exit(2)
    } finally {
      store.close()
    }
  }
}