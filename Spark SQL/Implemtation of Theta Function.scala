// Databricks notebook source
// Implementation using Normal Matrix Multiplication of XtX:
// Step 1 and 2: Computing XT_X:
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseMatrix, DenseVector, pinv}
val X = Seq(Array(1.0, 2.0),Array(2.0, 8.0), Array(4.0,9.0))
val y = Seq(Array(2.0), Array(2.0), Array(2.0))

// Convert the Matrix and Vector into RDD of key-value pairs:
val X_RDD : RDD[((Int, Int), Double)] = sc.parallelize(X.zipWithIndex.flatMap {case (row, i) => row.zipWithIndex.map { case (value, j) => ((i, j), value) }})
val y_RDD: RDD[(Int, Double)] = sc.parallelize(y.zipWithIndex.flatMap { case (array, i) => array.map(value => (i, value)) })

// Create the transpose of the matrix:
val X_trans = X_RDD.map { case ((i, j), value) => ((j, i), value) }

// Computing XT_X:
val X_transpose = X_trans.map { case ((j, i), value) => (i, (j, value)) }
X_transpose.persist()
val X_matrix = X_RDD.map { case ((k, j), value) => (k, (j, value)) }
val joined = X_transpose.join(X_matrix)
val product = joined.map { case (key, ((i, value1), (j, value2))) => ((i, j), value1 * value2) }
val XT_X = product.reduceByKey((m,n) => m + n)

// Step 3: Convert the result matrix to a Breeze Dense Matrix and compute pseudo-inverse:
// Convert RDD matrix to Breeze DenseMatrix
val result = XT_X.collect()
val denseMatrix = DenseMatrix(result)

// Determine the dimensions of the dense matrix
val maxRow = result.map { case ((i, _), _) => i }.max + 1
val maxCol = result.map { case ((_, j), _) => j }.max + 1

// Computing Pseudo Inverse:
val XT_X_Dense: DenseMatrix[Double] = DenseMatrix(denseMatrix.map(_._2).toArray).reshape(maxRow, maxCol)
val xTxInverse = pinv(XT_X_Dense)

// Step 4: Compute XT_Y and convert into Dense Vector:
val joined_matrix_vector = X_transpose.join(y_RDD)
val product_1 = joined_matrix_vector.map {case (_, ((k, value1), value2)) => (k, value1 * value2)}
val XT_Y = product_1.reduceByKey((m,n) => m + n)

val result_1 = XT_Y.collect()
val denseVector = DenseVector(result_1)
val XT_Y_Dense: DenseVector[Double] = DenseVector(denseVector.map(_._2).toArray)

// Step 5: Multiply xTxInverse with XT_Y:
val theta: DenseVector[Double] = xTxInverse * XT_Y_Dense
println("Theta: ")
println(theta)

// COMMAND ----------

// Implementation using Outer Product Method:(Bonus 1)
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseMatrix, DenseVector, pinv}

// Define the matrix X:
val X: RDD[Array[Double]] = sc.parallelize(Array(
  Array(1.0, 2.0),
  Array(2.0, 8.0),
  Array(4.0,9.0)
))

// Define the Vector y:
val y: RDD[(Int, Double)] = sc.parallelize(Seq(Array(2.0), Array(2.0), Array(2.0)).zipWithIndex.flatMap { case (array, i) => array.map(value => (i, value)) })

// Computing XTX using Outer Product Method:
val outerProducts: RDD[((Int, Int), Double)] = X.flatMap { row =>
  row.zipWithIndex.flatMap { case (value, i) =>
    row.zipWithIndex.map { case (value2, j) =>
      ((i, j), value * value2)
    }
  }
}
val XTX: RDD[((Int, Int), Double)] = outerProducts.reduceByKey(_ + _)

// Converting XTX into Dense Matrix and Computing Pseudo Inverse:
val result = XTX.collect()
val denseMatrix = DenseMatrix(result)

val maxRow = result.map { case ((i, _), _) => i }.max + 1
val maxCol = result.map { case ((_, j), _) => j }.max + 1

// Computing Pseudo Inverse:
val XT_X_Dense: DenseMatrix[Double] = DenseMatrix(denseMatrix.map(_._2).toArray).reshape(maxRow, maxCol)
val xTxInverse = pinv(XT_X_Dense)

// Computing XTy and converting into Dense Vector:
val X_transpose : RDD[(Int, (Int, Double))] = sc.parallelize(X.collect.zipWithIndex.flatMap {case (row, i) => row.zipWithIndex.map { case (value, j) => (i.toInt, (j.toInt, value)) }})

val joined_matrix_vector = X_transpose.join(y)
val product_1 = joined_matrix_vector.map {case (_, ((k, value1), value2)) => (k, value1 * value2)}
val XT_Y = product_1.reduceByKey((m,n) => m + n)

val result_1 = XT_Y.collect()
val denseVector = DenseVector(result_1)
val XT_Y_Dense: DenseVector[Double] = DenseVector(denseVector.map(_._2).toArray)

// Step 5: Multiply xTxInverse with XT_Y:
val theta: DenseVector[Double] = xTxInverse * XT_Y_Dense
println("Theta: ")
println(theta)
