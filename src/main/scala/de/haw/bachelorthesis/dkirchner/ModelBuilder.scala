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
    if (args.length < 1) {
      System.err.println("Usage: ModelBuilder <textfile>")
      System.exit(1)
    }

    val Array(textFile) = args.take(1)
    val sparkConf = new SparkConf()
      .setAppName("Model Builder")
      .set("spark.hadoop.validateOutputSpecs", "false") // to overwrite output files
    val sc = new SparkContext(sparkConf)

    checkMail(sc, textFile)
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

  def checkMail(sc: SparkContext, file: String): Unit = {
    val props = System.getProperties()
    props.setProperty("mail.store.protocol", "imaps")
    val session = Session.getDefaultInstance(props, null)
    val store = session.getStore("imaps")
    try {
      store.connect("imap.gmail.com", "danomonitoring@googlemail.com", "monitoring4Me!")
      val inbox = store.getFolder("Inbox")
      inbox.open(Folder.READ_WRITE)

      val messages = inbox.getMessages
      val rawContents = messages.map(msg => {
        if (msg.getContentType.isInstanceOf[Multipart])
          println(msg.getContent.asInstanceOf[Multipart].getCount)
          for (i <- 0 to msg.getContent.asInstanceOf[Multipart].getCount - 1) {
            val bodyPart = msg.getContent.asInstanceOf[Multipart].getBodyPart(i)
            println(bodyPart.getContentType.toString)
          }
        msg
      })
      /*#####val contents = rawContents.map(_..filter(_ >= ' ')).reduce((msg1, msg2) => msg1 + '\n' + msg2)
      //messages.foreach(_.setFlag(Flags.Flag.DELETED, true))

      println(contents.take(400))

      val conf = new Configuration()
      val hdfsCoreSitePath = new Path("/opt/hadoop/conf/core-site.xml")
      val hdfsHDFSSitePath = new Path("/opt/hadoop/conf/hdfs-site.xml")

      conf.addResource(hdfsCoreSitePath)
      conf.addResource(hdfsHDFSSitePath)

      val fileSystem = FileSystem.get(conf)

      val hdfsOutputStream = fileSystem.create(new Path("hdfs://192.168.206.131:54310/dev_emails_auto02.txt"))

      hdfsOutputStream.writeChars(contents)

      fileSystem.close() #####*/
      //val contentsRDD = sc.parallelize(contents)
      //contentsRDD.saveAsTextFile("hdfs://192.168.206.131:54310/dev_emails_auto01.txt")

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