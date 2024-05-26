// Databricks notebook source
// MAGIC %md
// MAGIC #### Create RDD

// COMMAND ----------

val ls = List(1,2,3,4,5)
val lsRDD = sc.parallelize(ls)

// COMMAND ----------

val rdd = sc.parallelize(1 to 10000)

// COMMAND ----------

val simple = sc.textFile("/FileStore/tables/simple.txt")

// COMMAND ----------

lsRDD.map(_+1).dependencies

// COMMAND ----------

lsRDD.map(_+1).toDebugString

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Laziness for Large-Scale Data
// MAGIC
// MAGIC * Spark leverages this by analyzing and optimizing the chain of operations before executing it.

// COMMAND ----------

val lastYearslogs: RDD[String] = sc.parallelize(List(
  "INFO: Application started",
  "ERROR: Invalid input detected",
  "WARNING: Connection timed out",
  "ERROR: Database connection failed",
  "INFO: Request completed successfully",
  "ERROR: File not found"
))
val firstlogsWithErrors = lastYearslogs.filter(_.contains("ERROR"))
firstlogsWithErrors.take(2)

// COMMAND ----------

// MAGIC %md
// MAGIC * The execution of filter is deferred until the 'take' action is applied.
// MAGIC * Spark will not compute intermediate RDD's. Instead, as soon as 2 elements of the filtered RDD have been computed, firstlogsWithErrors is done. 
// MAGIC * At this point Spark stops working, saving time and space computing elements of the unused result of filter.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### map and flatMap

// COMMAND ----------

val x = sc.textFile("/FileStore/tables/simple.txt")
x.map(line => line.split(",")).collect

// COMMAND ----------

x.flatMap(line => line.split(",")).collect

// COMMAND ----------

// MAGIC %md
// MAGIC ####groupByKey

// COMMAND ----------

val x = sc.parallelize(List(("A",1),("B",1),("C",1),("B",1),("C",1),("D",1),("C",1),("A",1)))
val x_group = x.groupByKey
x_group.collect

// COMMAND ----------

x_group.map{ case (k,v)=> (k, v.reduce(_+_))}.collect

// COMMAND ----------

x_group.mapValues(_.sum).collect

// COMMAND ----------

// MAGIC %md
// MAGIC ####reduceByKey

// COMMAND ----------

val y = sc.parallelize(List(("A",1),("B",1),("C",1),("B",1),("C",1),("D",1),("C",1),("A",1)))
y.reduceByKey(_+_).collect

// COMMAND ----------

import scala.util.Random

def generateKV(numKV: Int): RDD[(String, Int)] = {
  val keys = List.fill(numKV)((Random.nextInt(26) + 'A').toChar.toString)
  val values = List.fill(numKV)(Random.nextInt(10))
  val data = keys.zip(values)
  val rdd = sc.parallelize(data)
  rdd
}

val x = generateKV(10000000).cache

// COMMAND ----------

x.groupByKey.mapValues(_.sum).count

// COMMAND ----------

x.reduceByKey(_+_).count

// COMMAND ----------

// MAGIC %md
// MAGIC ####distinct

// COMMAND ----------

y.distinct.collect

// COMMAND ----------

// MAGIC %md
// MAGIC ####join

// COMMAND ----------

val x = sc.parallelize(List((3, "c"),(3, "f"), (1, "a"), (4,"d"), (1,"h"), (2, "b"), (5, "e"), (2, "g")), 3) 
val y = sc.parallelize(List((1, "A"),(2, "B"),(3,"C"),(2,"B"),(3,"C"),(4,"D"),(3,"C"),(1,"A")), 3)
x.join(y).collect

// COMMAND ----------

import org.apache.spark.HashPartitioner

val partitioner = new HashPartitioner(3)
val xPartitioned = x.partitionBy(partitioner)
val yPartitioned = y.partitionBy(partitioner)

// Print the partition ID and elements in each partition
xPartitioned.mapPartitionsWithIndex { case (partitionId, iter) =>
  Iterator(s"Partition $partitionId: ${iter.toList.mkString(", ")}")
}.collect.foreach(println)

// COMMAND ----------

xPartitioned.join(yPartitioned).collect

// COMMAND ----------

// MAGIC %md
// MAGIC ####cogroup

// COMMAND ----------

x.cogroup(y).collect

// COMMAND ----------

//broadcasting  example
val dataToBroadcast = 10
      
// Broadcast the data to all worker nodes
val broadcastData = sc.broadcast(dataToBroadcast)
      
val rdd = sc.parallelize(List(10, 20, 30, 40, 50))
      
// Use the broadcast variable within a Spark transformation
val result = rdd.map { num => 
                        val broadcastedValue = broadcastData.value
                        num * broadcastedValue
                      }
result.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Examples

// COMMAND ----------

//average of each key
val x = sc.textFile("/FileStore/tables/simple.txt")
    .map(line => { val a = line.split(","); (a(0).toInt, a(1).toDouble )})
    .groupByKey()
    .map{case (k,v) => ( k, v.sum / v.size)}
x.collect.foreach(println)

// COMMAND ----------

//average of each key
val x = sc.textFile("/FileStore/tables/simple.txt")
    .map(line => { val a = line.split(","); (a(0).toInt, a(1).toDouble )})
    .groupByKey()
    .mapValues(v => v.sum / v.size)
x.collect.foreach(println)

// COMMAND ----------

//sum of each key
val x = sc.textFile("/FileStore/tables/simple.txt")
    .map(line => { val a = line.split(","); (a(0).toInt, a(1).toDouble )})
    .reduceByKey(_+_)
x.collect.foreach(println)

// COMMAND ----------

val x = sc.textFile("/FileStore/tables/simple.txt")
  .map(line => {
    val a = line.split(",")
    (a(0).toInt, (a(1).toDouble, 1))  
  })
  .combineByKey(
    value => value,  
    (acc_local: (Double, Int), value) => (acc_local._1 + value._1, acc_local._2 + value._2),  //mergeValue
    (acc_d_1: (Double, Int), acc_d_2: (Double, Int)) => (acc_d_1._1 + acc_d_2._1, acc_d_1._2 + acc_d_2._2)  //mergeCombiner
  )
  .mapValues { case (sum, count) => sum / count } 
x.collect.foreach(println)

// COMMAND ----------

//Join
val emps = sc.textFile("/FileStore/tables/e.txt")
  .map(line => { val a = line.split(",")
    (a(0), a(1).toInt, a(2)) } )
    
emps.collect.foreach(println)
val depts = sc.textFile("/FileStore/tables/d.txt")
  .map(line => { val a = line.split(",")
  (a(0), a(1).toInt )})
depts.collect.foreach(println)

val res = emps.map(e => (e._2, e)).join(depts.map(d => (d._2, d)))
  .map{case (k, (e, d)) => e._1+" "+d._1}
res.collect.foreach(println)

// COMMAND ----------

case class Employee (name: String, dno: Int, address: String)
case class Department (name: String, dno: Int)

val emps = sc.textFile("/FileStore/tables/e.txt")
  .map(line => { val a = line.split(",")
    Employee(a(0), a(1).toInt, a(2)) } )
val depts = sc.textFile("/FileStore/tables/d.txt")
  .map(line => { val a = line.split(",")
    Department(a(0), a(1).toInt )})
val res = emps.map(e => (e.dno, e))
                .join(depts.map(d => (d.dno, d)))
                  .map{case (k, (e, d)) => e.name+" "+d.name}
res.collect.foreach(println)
