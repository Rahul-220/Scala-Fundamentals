// #### Create RDD

val ls = List(1,2,3,4,5)
val lsRDD = sc.parallelize(ls)


val rdd = sc.parallelize(1 to 10000)


val simple = sc.textFile("/FileStore/tables/simple.txt")


lsRDD.map(_+1).dependencies


lsRDD.map(_+1).toDebugString

//  ##### Laziness for Large-Scale Data
//  Spark leverages this by analyzing and optimizing the chain of operations before executing it.

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


//  * The execution of filter is deferred until the 'take' action is applied.
//  * Spark will not compute intermediate RDD's. Instead, as soon as 2 elements of the filtered RDD have been computed, firstlogsWithErrors is done. 
//  * At this point Spark stops working, saving time and space computing elements of the unused result of filter.

//  ##### map and flatMap


val x = sc.textFile("/FileStore/tables/simple.txt")
x.map(line => line.split(",")).collect


x.flatMap(line => line.split(",")).collect


// ####groupByKey


val x = sc.parallelize(List(("A",1),("B",1),("C",1),("B",1),("C",1),("D",1),("C",1),("A",1)))
val x_group = x.groupByKey
x_group.collect


x_group.map{ case (k,v)=> (k, v.reduce(_+_))}.collect


x_group.mapValues(_.sum).collect



// ####reduceByKey


val y = sc.parallelize(List(("A",1),("B",1),("C",1),("B",1),("C",1),("D",1),("C",1),("A",1)))
y.reduceByKey(_+_).collect


import scala.util.Random

def generateKV(numKV: Int): RDD[(String, Int)] = {
  val keys = List.fill(numKV)((Random.nextInt(26) + 'A').toChar.toString)
  val values = List.fill(numKV)(Random.nextInt(10))
  val data = keys.zip(values)
  val rdd = sc.parallelize(data)
  rdd
}

val x = generateKV(10000000).cache



x.groupByKey.mapValues(_.sum).count


x.reduceByKey(_+_).count



//  ####distinct


y.distinct.collect

//  ####join


val x = sc.parallelize(List((3, "c"),(3, "f"), (1, "a"), (4,"d"), (1,"h"), (2, "b"), (5, "e"), (2, "g")), 3) 
val y = sc.parallelize(List((1, "A"),(2, "B"),(3,"C"),(2,"B"),(3,"C"),(4,"D"),(3,"C"),(1,"A")), 3)
x.join(y).collect


import org.apache.spark.HashPartitioner

val partitioner = new HashPartitioner(3)
val xPartitioned = x.partitionBy(partitioner)
val yPartitioned = y.partitionBy(partitioner)

// Print the partition ID and elements in each partition
xPartitioned.mapPartitionsWithIndex { case (partitionId, iter) =>
  Iterator(s"Partition $partitionId: ${iter.toList.mkString(", ")}")
}.collect.foreach(println)


xPartitioned.join(yPartitioned).collect

// ####cogroup


x.cogroup(y).collect


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

//  #### Examples


//average of each key
val x = sc.textFile("/FileStore/tables/simple.txt")
    .map(line => { val a = line.split(","); (a(0).toInt, a(1).toDouble )})
    .groupByKey()
    .map{case (k,v) => ( k, v.sum / v.size)}
x.collect.foreach(println)


//average of each key
val x = sc.textFile("/FileStore/tables/simple.txt")
    .map(line => { val a = line.split(","); (a(0).toInt, a(1).toDouble )})
    .groupByKey()
    .mapValues(v => v.sum / v.size)
x.collect.foreach(println)


//sum of each key
val x = sc.textFile("/FileStore/tables/simple.txt")
    .map(line => { val a = line.split(","); (a(0).toInt, a(1).toDouble )})
    .reduceByKey(_+_)
x.collect.foreach(println)


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
