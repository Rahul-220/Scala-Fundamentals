
//  ## Part 1 Matrix Multiplicaiton of Coordinate Matrices

//  ### 1.1 Small Matrices


// Define two small matrices M and N
val M = Array(Array(1.0, 2.0), Array(3.0, 4.0))
val N = Array(Array(5.0, 6.0), Array(7.0, 8.0))


//// Convert the small matrices to RDDs
val M_RDD_Small = sc.parallelize(M.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })
val N_RDD_Small = sc.parallelize(N.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })


//  #### 1.1.2 [TODO] Function to perform matrix multiplication of RDDs


def COOMatrixMultiply ( M: RDD[((Int,Int),Double)], N: RDD[((Int,Int),Double)] ): RDD[((Int,Int),Double)] = {
 // Matrix multiplication using RDDs
  val M_matrix = M.map { case ((i, k), value) => (k, (i, value)) }
  val N_matrix = N.map { case ((k, j), value) => (k, (j, value)) }
  val joined_matrices = M_matrix.join(N_matrix)
  val product = joined_matrices.map { case (key, ((i, value1), (j, value2))) => ((i, j), value1 * value2) }
  val result = product.reduceByKey((m,n) => m + n)
  return result
}


val R_RDD_Small = COOMatrixMultiply(M_RDD_Small, N_RDD_Small)
R_RDD_Small.collect.foreach(println)

//  #### 1.1.3 Validate the result

def manualMatrixMultiply(M: Array[Array[Double]], N: Array[Array[Double]]): Array[Array[Double]] = {
  val result = Array.ofDim[Double](M.length, N(0).length)
  for {
    i <- M.indices
    j <- N(0).indices
    k <- M(0).indices
  } result(i)(j) += M(i)(k) * N(k)(j)
  result
}

val result_manual = R_RDD_Small.collect().map { case ((i, j), value) => (i, j, value) }.sortBy { case (i, j, _) => (i, j) }

val resultArray = Array.ofDim[Double](M.length, N(0).length)

for ((i, j, value) <- result_manual) {
  resultArray(i)(j) = value
}

val expectedResult = manualMatrixMultiply(M, N)
assert(resultArray.deep == expectedResult.deep, "Result mismatch")


//  ### 1.2 Large Datasets
//  #### 1.2.1 Generate Random Coordinate Matrices

import scala.util.Random

// Function to generate random coordinate matrices for large datasets
def randomCOOMatrix ( n: Int, m: Int ): RDD[((Int,Int),Double)] = {
  val max = 10
  val l = Random.shuffle((0 until n).toList)
  val r = Random.shuffle((0 until m).toList)
  sc.parallelize(l)
    .flatMap{ i => val rand = new Random()
              r.map{ j => ((i.toInt,j.toInt),rand.nextDouble()*max) } 
              }
}


//  #### 1.2.2 [TODO] Set the Dimensions of the Matrices
//Set the dimensions of matrices M and N as 1024x1024.
val n = 1024
val m = 1024
val M_RDD_Large = randomCOOMatrix(n,m)
val N_RDD_Large = randomCOOMatrix(m,n)


//Perform Multiplication
val R_RDD_Large = COOMatrixMultiply(M_RDD_Large, N_RDD_Large)


// Notice the time it takes to multiply two 1024x1024 matrices
R_RDD_Large.count

//  ## Part 2 Matrix Multiplication of Block Matrices
//  #### 2.1 Small Block Matrix Multiplication


import org.apache.spark.mllib.linalg.distributed._

// Convert small coordinate matrices to block matrices
val M_Block_Matrix = new CoordinateMatrix(M_RDD_Small.map { case ((i, j), value) => MatrixEntry(i, j, value)}).toBlockMatrix()
val N_Block_Matrix = new CoordinateMatrix(N_RDD_Small.map { case ((i, j), value) => MatrixEntry(i, j, value)}).toBlockMatrix()


M_Block_Matrix.blocks.collect.foreach(println)


N_Block_Matrix.blocks.collect.foreach(println)


// Perform multiplication of small block matrices
val R_Block_Small = M_Block_Matrix.multiply(N_Block_Matrix)


R_Block_Small.blocks.collect.foreach(println)


//  ### 2.2 Large Block Multiplication
//  #### [TODO] Set the Block Size

//set the number of rows and columns per block to be 64
val r_b = 64
val c_b = 64


//// Convert large coordinate matrices to block matrices where each block has 64 rows and 64 columns
val M_Block_Matrix_Large = new CoordinateMatrix(M_RDD_Large.map { case ((i, j), value) => MatrixEntry(i, j, value)}).toBlockMatrix(r_b,c_b)
val N_Block_Matrix_Large = new CoordinateMatrix(N_RDD_Large.map { case ((i, j), value) => MatrixEntry(i, j, value)}).toBlockMatrix(c_b,c_b)


M_Block_Matrix_Large.validate
N_Block_Matrix_Large.validate


assert(M_Block_Matrix_Large.numRowBlocks == 16, "Result mismatch")
assert(M_Block_Matrix_Large.numColBlocks == 16, "Result mismatch")


assert(N_Block_Matrix_Large.numRowBlocks == 16, "Result mismatch")
assert(N_Block_Matrix_Large.numColBlocks == 16, "Result mismatch")


val R_Block_Large = M_Block_Matrix_Large.multiply(N_Block_Matrix_Large)


// Notice the time it takes to multiply two 1024x1024 block matrices
R_Block_Large.blocks.count
