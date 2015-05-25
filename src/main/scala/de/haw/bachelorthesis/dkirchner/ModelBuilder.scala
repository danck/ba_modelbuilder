package de.haw.bachelorthesis.dkirchner

/**
 *
 * Created by Daniel on 12.05.2015.
 */

import java.io._
import java.util.Calendar
import javax.activation.MailcapCommandMap

import javax.mail._
import javax.mail.internet._
import javax.mail.search._
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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
    if (args.length < 3) {
      System.err.println("Usage: ModelBuilder <textfile> <mail account> <mail password>")
      System.exit(1)
    }

    val Array(textFile, account, password) = args.take(3)
    val sparkConf = new SparkConf()
      .setAppName("Model Builder")
      .set("spark.hadoop.validateOutputSpecs", "false") // to overwrite output files
    val sc = new SparkContext(sparkConf)

    val newMessages = MailService.fetchFrom(account, password)
    println(newMessages)
    System.exit(0)

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
    val oos = new ObjectOutputStream(new FileOutputStream("/tmp/tfidf"))
    try {
      oos.writeObject(relevanceVector)
    } catch {
      case e: Exception => println("Exception while saving model:")
        e.printStackTrace()
        System.exit(1)
    } finally {
      oos.close()
    }

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
      throw new IllegalArgumentException("Input vectors must be of equal size")

    val indices1 = sv1.getIndices
    val indices2 = sv2.getIndices

    val indices = indices1.union(indices2).distinct.sorted
    val values = indices.map(index => sv1.apply(index) + sv2.apply(index))

    val result = Vectors.sparse(sv1.size, indices, values)

    result
  }

  def fetchMail(account: String, password: String): Unit = {
    val props = System.getProperties
    props.setProperty("mail.store.protocol", "imaps")
    val session = Session.getDefaultInstance(props, null)
    val store = session.getStore("imaps")
    val messageTexts: StringBuilder = new StringBuilder

    try {
      store.connect("imap.gmail.com", account, password)
      val inbox = store.getFolder("Inbox")
      inbox.open(Folder.READ_WRITE)

      val messages = inbox.getMessages
      var rawText = new String
      var counter = 0

      messages.foreach(msg => {
        rawText = ""
        try {


          if (msg.getContent.isInstanceOf[Multipart]) {
            val multiPartMessage = msg.getContent.asInstanceOf[Multipart]
            for (i <- 0 to multiPartMessage.getCount - 1) {
              if (multiPartMessage.getBodyPart(i).getContent.isInstanceOf[String]) {
                val rawText = multiPartMessage.getBodyPart(i).getContent.asInstanceOf[String]
              }
            }
          }


          if (msg.getContent.isInstanceOf[String]) {
            rawText = msg.getContent.asInstanceOf[String]
          }


          if (rawText != "") {
            val bodyString = rawText
            val bodyLines = bodyString.split('\n')
              .filter(line => !line.trim.startsWith(">")) // remove quoted lines
              .filter(line => !line.trim.startsWith("<")) // remove html tags
              .filter(line => !line.trim.startsWith("On"))
            val cleanLines = bodyLines.map(line => line.stripLineEnd) // remove newlines
            val cleanText = if (cleanLines.nonEmpty)
                cleanLines.reduce(_ + _).replaceAll("[^a-zA-Z0-9]", " ") // remove special characters
              else ""

            counter += 1
            println( counter.toString )
            messageTexts.append(cleanText + "\n")
          }
        } catch {
          case uee: UnsupportedEncodingException =>  //continue
        }
      })

      inbox.close(true)
    } catch {
      case e: NoSuchProviderException => e.printStackTrace()
        System.exit(1)
      case me: MessagingException     => me.printStackTrace()
        System.exit(2)
    } finally {
      store.close()
    }
    messageTexts.toString()
  }
}