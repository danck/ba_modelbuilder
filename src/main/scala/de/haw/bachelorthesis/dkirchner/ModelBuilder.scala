package de.haw.bachelorthesis.dkirchner

/**
 *
 * Created by Daniel on 12.05.2015.
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

/**
 *
 */
object ModelBuilder {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Model Builder").setMaster("local")
    val sc = new SparkContext(sparkConf)
    println("SUCCESS")
  }
}