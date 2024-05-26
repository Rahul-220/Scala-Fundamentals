// #### Create DataFrame

val tupleRDD = sc.parallelize(Seq((1, "Alice", 16, "Arlington"), 
                                  (2, "Bob", 18, "Brownsville"), 
                                  (3, "Carrol", 20, "Corpus Christi"), 
                                  (4, "Dave", 26, "Dallas"), 
                                  (5, "Eve", 24, "El Paso"),
                                  (6, "Frank", 22, "Fort Worth")))


// Create DataFrame from an RDD without arguments

val tupleDF = tupleRDD.toDF
tupleDF.show

tupleDF.printSchema()

//  Create DataFrame from an RDD with arguments

val tupleDF = tupleRDD.toDF("id", "name", "age", "city").show

//  Create DataFrame from an RDD with case class


case class Person(id: Int, name: String, age: Int, city: String)
val peopleRDD = sc.parallelize(Seq(Person(1, "Alice", 16, "Arlington"), 
                                   Person(2, "Bob", 18, "Brownsville"), 
                                   Person(3, "Carrol", 20, "Corpus Christi"), 
                                   Person(4, "Dave", 26, "Dallas"), 
                                   Person(5, "Eve", 24, "El Paso"), 
                                   Person(6, "Frank", 22, "Fort Worth")))
val peopleDF = peopleRDD.toDF
peopleDF.show
peopleDF.printSchema()

// Create Dataframe from an RDD with schema explicitly specified


import org.apache.spark.sql.types._

//Define schema
val schema = StructType(Seq(
  StructField("id", IntegerType, nullable = true),
  StructField("name", StringType, nullable = true),
  StructField("age", IntegerType, nullable = true),
  StructField("city", StringType, nullable = true)
))
// Convert records of the RDD (people) to Rows
val rowRDD = tupleRDD.map(tuple => Row.fromTuple(tuple))
// Apply the schema to the RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)
peopleDF.printSchema


val schema2 = "Country string, LifeExp float, Region string"

val df1 = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "false")
  .schema(schema2)
  .load("dbfs:/FileStore/tables/LifeExp.csv")

df1.printSchema


import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
  

display(dbutils.fs.ls("dbfs:/FileStore/tables/"))


val simpleDF = spark.read.csv("dbfs:/FileStore/tables/simple.txt")


simpleDF.createOrReplaceTempView("simple")


simpleDF.show


val df = spark.read.option("header", "true").csv("/FileStore/tables/header_flights_abbr.csv")
display(df)

// #### Running SQL Queries


peopleDF.createOrReplaceTempView("people")


val adultsDF = spark.sql("SELECT * FROM people WHERE age > 18")
adultsDF.show()

// Specify column using $

peopleDF.filter($"age" > 18).show


// Specify column using the DataFrame Reference


peopleDF.filter(peopleDF("age") > 18).show

// Specify column using SQL string

peopleDF.filter("age > 18").show


val adultDF = peopleDF.select("id", "name")
                      .where("city == 'Arlington'")
adultDF.show

peopleDF.groupBy("age").count().show()

// #### Views


peopleDF.createGlobalTempView("people")


// Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()


// Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()


peopleDF.createOrReplaceTempView("people2")


spark.sql("SELECT * FROM people2").show()

// ##### null values

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// Creating an RDD with null values
val rdd = sc.parallelize(Seq(
  Row(1, "Alice", null, "Arlington"),
  Row(2, "Bob", 20, "Brownsville"),
  Row(3, "Carrol", 30, null),
  Row(null, null, null, null)
))

// Creating the DataFrame from the RDD and schema
val peopleDF2 = spark.createDataFrame(rdd, schema)
peopleDF2.show


val removeNullDF = peopleDF2.na.drop().show


val removeNullDF2 = peopleDF2.na.drop("all").show


val removeNullDF3 = peopleDF2.na.drop(Array("age")).show


val filledDF = peopleDF2.na.fill(0)
filledDF.show()


val filledDF2 = peopleDF2.na.fill(Map(
  "age" -> 0,
  "city" -> "Unknown"
))

// Displaying the result
filledDF2.show()

val filledDF3 = peopleDF2.na.replace(Array("city"), Map("Arlington" -> "New Name"))

// Displaying the result
filledDF3.show()

// ##### Actions


filledDF3.collect
filledDF3.count
filledDF3.first
filledDF3.take(2)
filledDF3.show(2)


// ##### Joins


case class People(id: Int, v: String)
val p = List(People(1, "Alice"), People(2, "Bob"), People(3, "Carol"))
val peopleDF = sc.parallelize(p).toDF

case class Loc(id: Int, v: String)
val l = List(Loc(1, "Central Texas"), Loc(2, "South Texas"))
val locationsDF = sc.parallelize(l).toDF

peopleDF.show
locationsDF.show

val trackPeopleDF = peopleDF.join(locationsDF, peopleDF("id") === locationsDF("id")).show


val trackPeopleDF2 = peopleDF.join(locationsDF, peopleDF("id")===locationsDF("id"), "left_outer").show


val trackPeopleDF2 = peopleDF.join(locationsDF, peopleDF("id")===locationsDF("id"), "right_outer").show


val trackPeopleDF3 = locationsDF.filter($"v"==="South Texas").show


val trackPeopleDF3 = locationsDF.filter($"state"==="South Texas")


spark.conf.set("spark.sql.shuffle.partitions", "2")

val distributeDF
  = spark.sql("SELECT * FROM people distribute by age")
distributeDF.show()


val clusterDF
  = spark.sql("SELECT * FROM people cluster by age")
clusterDF.show()


val explode = spark.sql("SELECT explode(array(10, 20))")
explode.show

//##### User-Defined Function (UDF)

val random = udf(() => Math.random())
spark.udf.register("random", random)
spark.sql("SELECT random()").show()


// Define and register a one-argument UDF
val plusOne = udf((x: Int) => x + 1)
spark.udf.register("plusOne", plusOne)
spark.sql("SELECT plusOne(5)").show()


// UDF in a WHERE clause
spark.udf.register("oneArgFilter", (n: Int) => { n > 5 })
spark.range(1, 10).createOrReplaceTempView("test")
spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show()


// ##### UDAF

display(dbutils.fs.ls("/databricks-datasets/"))


import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions

case class Average(var sum: Long, var count: Long)

object MyAverage extends Aggregator[Long, Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, data: Long): Average = {
    buffer.sum += data
    buffer.count += 1
    buffer
  }
  // Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  // Transform the output of the reduction
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

// Register the function to access it
spark.udf.register("myAverage", functions.udaf(MyAverage))

val df = spark.read.json("dbfs:/FileStore/tables/employees.json")
df.createOrReplaceTempView("employees")
df.show()

val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
result.show()
