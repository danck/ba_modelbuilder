package de.haw.bachelorthesis.dkirchner.tests

import org.scalatest.FunSuite
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector}

/**
 * Created by Daniel on 24.05.2015.
 */
class ModelBuilderTests extends FunSuite {
  val vector1 = Vectors.sparse(2,Array(0),Array(0.1)).asInstanceOf[SparseVector]
  val vector2 = Vectors.sparse(2,Array(1),Array(0.1)).asInstanceOf[SparseVector]
  val expected1 = Vectors.sparse(2,Array(0, 1),Array(0.1, 0.1)).asInstanceOf[SparseVector]

  val vector3 = Vectors.sparse(2,Array(1),Array(0.1)).asInstanceOf[SparseVector]
  val vector4 = Vectors.sparse(2,Array(1),Array(0.1)).asInstanceOf[SparseVector]
  val expected2 = Vectors.sparse(2,Array(1),Array(0.2)).asInstanceOf[SparseVector]

  test("distinct: mergeSparseVectors(vector1, vector2)") {
    val result1 = de.haw.bachelorthesis.dkirchner.ModelBuilder.mergeSparseVectors(vector1, vector2)
    assert(result1.equals(expected1))
  }

  test("non-distinct: mergeSparseVectors(vector3, vector4)") {
    val result2 = de.haw.bachelorthesis.dkirchner.ModelBuilder.mergeSparseVectors(vector3, vector4)
    println(result2.toString)
    assert(result2.equals(expected2))
  }
}
