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
 *
 */
object ModelBuilder {
  // number of messages to build the relevance model from
  private val docWindowSize: Integer = 500

  // time between updating the relevance model
  private val refreshInterval: Long = 10000

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
    HDFSService.appendToTextFile(textFile, newMessages)

    val documents: RDD[Seq[String]] = sc.textFile(textFile)
      .filter(_.length > 15)
      .map(_.toLowerCase)
      .map(_.split(" ").toSeq)

    val hashingTF = new HashingTF(1 << 20)
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()

    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val relevanceVector = tfidf
      .take(docWindowSize)
      .reduce((vector1, vector2) =>
        addSparseVectors(vector1.asInstanceOf[SparseVector], vector2.asInstanceOf[SparseVector])
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

    result
  }
}