package de.haw.bachelorthesis.dkirchner

/*
 * This file is part of my bachelor thesis.
 *
 * Copyright 2015 Daniel Kirchner <daniel.kirchner1@haw-hamburg.de>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the
 * Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */

import java.io._
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector}


/**
 *  Entry class for model builder. This component periodically fetches new emails and
 *  appends the extracted message bodies to a corpus of older messages.<br/>
 *  It then scores the words from each message by building a IDF-vector from the corpus
 *  and then applying it to a TF-vector of the latest <code>n</code> messages
 *  (<code>n</code> being the window size).
 */
object ModelBuilder {
  // number of messages to build the relevance model from
  private val docWindowSize: Integer = 500

  // time between updating the relevance model
  private val refreshInterval: Long = 10000

  // local file system path to save the feature vector at
  private val modelPath: String = "/tmp/tfidf"

  def main (args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ModelBuilder <textfile> <mail account> <mail password>")
      System.exit(1)
    }
    val Array(textFile, account, password) = args.take(3)

    val sparkConf = new SparkConf()
      .setAppName("Model Builder")
    val sc = new SparkContext(sparkConf)

    val beginMessageRetrieval = System.currentTimeMillis()
    val newMessages = MailService.fetchFrom(account, password)
    val finishMessageRetrieval = System.currentTimeMillis()

    HDFSService.appendToTextFile(textFile, newMessages)

    val beginFeatureExtraction = System.currentTimeMillis()

    val documents: RDD[Seq[String]] = sc.textFile(textFile)
      .map(_.toLowerCase)
      .map(_.split(" ").filter(_.length > 2).toSeq)

    documents.cache()

    val hashingTF = new HashingTF(1 << 20)
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache() // keep the tf cached because we will be using it twice

    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val relevanceVector = tfidf
      .take(docWindowSize)
      .reduce((vector1, vector2) =>
        addSparseVectors(vector1.asInstanceOf[SparseVector], vector2.asInstanceOf[SparseVector])
      )

    val finishFeatureExtraction = System.currentTimeMillis()

    // write the model instance out to a file
    val oos = new ObjectOutputStream(new FileOutputStream(modelPath))
    try {
      oos.writeObject(relevanceVector)
    } catch {
      case e: Exception =>
        println("Error while saving model: ")
        e.printStackTrace()
        System.exit(1)
    } finally {
      oos.close()
    }

    // Report
    documents.foreach(line => println(line))
    println("#### UPDATED AT " + Calendar.getInstance().getTime + " ####")
    println("# Generated new feature vector with " +
      relevanceVector.asInstanceOf[SparseVector].getIndices.size + " non-zero entries  (" +
      ((relevanceVector.asInstanceOf[SparseVector].getIndices.size.toDouble / relevanceVector.size) * 100)
      + "% density)")
    println("# New vector saved at: " + modelPath)
    println("# Updated document corpus at: " + textFile)
    println("# Total time for message retrieval: " +
      (finishMessageRetrieval - beginMessageRetrieval).toDouble/1000 + " seconds" )
    println("# Total time for feature extraction: " +
      (finishFeatureExtraction - beginFeatureExtraction).toDouble/1000 + " seconds" )
  }

  /**
   * Extends the class Spark SparseVector implementation by attribute getters.
   * This is a helper class for the local method addSparseVectors
   * @param sv Regular SparseVector to be extended
   */
  private implicit class UnifiableSparseVector(sv: SparseVector) {
    def unifiableSparseVector: (Int, Array[Int], Array[Double]) = {
      (sv.size, sv.indices, sv.values)
    }

    def getIndices = sv.indices
    def getValues = sv.values
  }

  /**
   * Adds two sparse vectors of the same length by adding their values at
   * each index
   * @param sv1 unifiableSparseVector
   * @param sv2 unifiableSparseVector
   * @return Vector that is the union of sv1 and sv2
   */
  private[dkirchner] def addSparseVectors(sv1: SparseVector, sv2: SparseVector): Vector = {
    if (sv1.size != sv2.size)
      throw new IllegalArgumentException("Input vectors must be of equal size")

    val indices1 = sv1.getIndices
    val indices2 = sv2.getIndices

    val indices = indices1.union(indices2).distinct.sorted
    val values = indices.map(index => sv1.apply(index) + sv2.apply(index))

    val result = Vectors.sparse(sv1.size, indices, values)

    return result
  }
}