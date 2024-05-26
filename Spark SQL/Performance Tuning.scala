// #### Catalog

val df = Seq((1, "andy"), (2, "bob"), (2, "andy")).toDF("count", "name")                                                    
df.createOrReplaceTempView("temp_view1")
display(spark.catalog.listTables)

spark.conf.set("spark.databricks.io.cache.enabled", "false")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

 
// #### Catalyst: Predicate Pushdown

val empDF = spark.read.parquet("dbfs:/FileStore/tables/emp_snappy.parquet/")
val deptDF = spark.read.parquet("dbfs:/FileStore/tables/dept_snappy.parquet/")

spark.read.parquet("dbfs:/FileStore/tables/emp_snappy.parquet/").createOrReplaceTempView("emp_view")
spark.read.parquet("dbfs:/FileStore/tables/dept_snappy.parquet/").createOrReplaceTempView("dept_view")

empDF.show(2)
deptDF.show(2)

//In first 2 Plans Catalyst does JOIN, then does FILTER, but in Optimized Plan, Catalyst moves the FILTER before the JOIN for better Performance
val result = spark.sql("SELECT e.last_name, d.dept FROM emp_view e INNER JOIN dept_view d ON e.dept=d.dept WHERE e.dept = 301").explain(true)


//  #### Catalyst: Change Join Strategy

spark.read.format("parquet").load("dbfs:/FileStore/tables/cops_02_snappy.parquet").createOrReplaceTempView("cops_view1")
spark.read.format("parquet").load("dbfs:/FileStore/tables/cops_03_snappy.parquet").createOrReplaceTempView("cops_view2")

// Here we use the 'explain' method so don't have to go to Spark UI 'SQL' tab. 
// Without a Hint, Catalyst uses Join Strategy = 'SortMergeJoin'
spark.sql("SELECT a.Category, b.PdDistrict FROM cops_view1 a JOIN cops_view2 b ON a.Time = b.Time").explain(true)

display(dbutils.fs.ls("dbfs:/FileStore/tables"))

// Tell Catalyst it's OK to do BroadcastHashJoin on Table up to 50MB 
spark.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")

// Tell Catalyst to broadcast 'cops_view2' table
spark.sql("SELECT /*+ BROADCAST(cops_view2) */ * FROM cops_view1 a JOIN cops_view2 b ON a.Time = b.Time").explain(true)

// #### Catalyst: Column Pruning

import org.apache.spark.sql.types.{StructType, StructField, StringType}

val policeSchema = new StructType()
  .add(StructField("IncidentNum", StringType, true))
  .add(StructField("Category", StringType, true))
  .add(StructField("Description", StringType, true))
  .add(StructField("DayOfWeek", StringType, true))
  .add(StructField("Date", StringType, true))
  .add(StructField("Time", StringType, true))
  .add(StructField("PdDistrict", StringType, true))
  .add(StructField("Resolution", StringType, true))
  .add(StructField("Address", StringType, true))
  .add(StructField("X", StringType, true))
  .add(StructField("Y", StringType, true))
  .add(StructField("Loc", StringType, true))
  .add(StructField("PdId", StringType, true))

val CSVColPruneDF = spark.read.schema(policeSchema).csv("dbfs:/FileStore/tables/sfpd1/sf101")
display(CSVColPruneDF)

CSVColPruneDF.write.format("parquet").mode("overwrite").save("/tmp/parquet_colPrune/")
val ParquetColPruneDF = spark.read.format("parquet").load("/tmp/parquet_colPrune/")

 
display(CSVColPruneDF.select("Category", "Description"))
//Go to Spark UI->SQL Tab-> Click the link under Description-> Click Scan CSV-> Check size of files read

 
display(ParquetColPruneDF.select("Category", "Description"))
//Go to Spark UI->SQL Tab-> Click the link under Description-> Click Scan Parquet-> Check size of files read

// #### Tungsten: Improved Memory Usage

// Do this first to prevent side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

import org.apache.spark.storage.StorageLevel

// View 'Storage' tab in Spark UI to view RAM size consumed between RDD and DataFrame

val rdd1 = sc.textFile("dbfs:/FileStore/tables/autos.csv").map(_.split(","))
val rdd2 = rdd1.persist(StorageLevel.MEMORY_ONLY_SER)
rdd2.count()

val df1 = spark.read.option("header" , "true").option("inferSchema", "true").csv("dbfs:/FileStore/tables/autos.csv")
val df2 = df1.persist(StorageLevel.MEMORY_ONLY_SER)
df2.count()

// #### Tungsten: Whole Stage Code Generation

spark.range(1000).filter("id > 100").selectExpr("sum(id)").show()
//Go to SQL tab-> Click the job under Description and see WholeStageCodeGen fusing the operations

// #### Adaptive Query Optimization

// ##### AQE: Coalesce Shuffle Partitions

// Disable BroadcastHashJoins to force a SortMergeJoin
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)  

// Disable AQE
spark.conf.set("spark.sql.adaptive.enabled",false)

// Force # of Shuffle Partitions = 50 as MAX. Default = 200
spark.conf.set("spark.sql.shuffle.partitions", 50)

spark.read.format("parquet").load("dbfs:/FileStore/tables/sample_parq/").createOrReplaceTempView("sample")

spark.read.format("parquet").load("dbfs:/FileStore/tables/lookup_parq/").createOrReplaceTempView("lookup")

// Notice 'company_id' = 1 will be a large Partition when compared to rest when JOIN on 'company_id' column
display(spark.sql("SELECT company_id, count(tx_id) as transactions FROM sample GROUP BY company_id ORDER BY transactions DESC LIMIT 10"))

// Here's other Table we will be JOINing
display(spark.sql("SELECT * FROM lookup ORDER BY id"))


//Notice Hint to force SortMergeJoin (another Spark 3.x functionality) 
//From Spark UI -> SQL -> -> Click the job under Description -> Details  
//Notice Lack of 'AQEShuffleRead' in DAG, only says 'Exchange'

display(spark.sql("SELECT /*+MERGE(sample, lookup)*/sample.tx_id, lookup.company FROM sample JOIN lookup ON sample.company_id = lookup.id"))

//Enable both AQE and Coalesce Partitions
spark.conf.set("spark.sql.adaptive.enabled", true)

// When true and spark.sql.adaptive.enabled = true, Spark will coalesce contiguous shuffle partitions according to the target size 
// (specified by 'spark.sql.adaptive.advisoryPartitionSizeInBytes'), to avoid too many small tasks.
spark.conf.set("spark.sql.adaptive.coalescePartitions", true)

 
//Drop HINT to force SORT MERGE JOIN (instead of BroadcastHashJoin)
//With AQE, it will Coalesce Shuffle Partitions
////From Spark UI -> SQL -> -> Click the job under Description -> Details  
//Notice 'AQEShuffleRead'
display(spark.sql("SELECT /*+MERGE(sample, lookup)*/ sample.tx_id, lookup.company, sample.field1 FROM sample JOIN lookup ON sample.company_id = lookup.id"))


//  ##### AQE: Converting SortMergeJoin to BroadcastHashJoin

display(spark.read.parquet("dbfs:/FileStore/tables/emp_snappy.parquet/"))
display(spark.read.format("parquet").load("dbfs:/FileStore/tables/dept_snappy.parquet/"))

val mpDF = spark.read.format("parquet").load("dbfs:/FileStore/tables/emp_snappy.parquet/")
val deptDF = spark.read.format("parquet").load("dbfs:/FileStore/tables/dept_snappy.parquet/")

// Convert DF into Spark Views          
empDF.createOrReplaceTempView("emp_view")
deptDF.createOrReplaceTempView("dept_view")

//Turn off both BroadcastHashJoins and AQE
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)  
spark.conf.set("spark.sql.adaptive.enabled",false)

//SortMergeJoin (Spark UI-> SQL tab -> click on the job under Description-> notice SortMergeJoin)
display(empDF.join(deptDF, "dept").select("last_name", "dept", "dept_name").limit(4))

//Turn on BroadcastHashJoin and AQE and execute again.
//         (Open any Job -> SQL -> Click on Description)
//         Did Performance Improve based on Clock time compared to SortMergeJoin?
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
//spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10)
spark.conf.set("spark.sql.adaptive.enabled",true)

display(empDF.join(deptDF, "dept").select("last_name", "dept", "dept_name"))

 
// ##### AQE: Optimizing Skew Join

spark.conf.set("spark.sql.shuffle.partitions", 200)
val t1DF = spark.read.parquet("dbfs:/FileStore/tables/t1/")
t1DF.createOrReplaceTempView("t1_view")
t1DF.show


val t2DF = spark.read.parquet("dbfs:/FileStore/tables/t2/")
t2DF.createOrReplaceTempView("t2_view")
t2DF.show


val resultDF = spark.sql("""
  SELECT make, model, COUNT(*) AS cnt
  FROM t2_view
  GROUP BY make, model
  ORDER BY cnt DESC
""")

resultDF.show()


//View Spark UI-> Stages-> Check Shuffle read and write and duration of the jobs.  It's a Skew Partition issue (2 minute query)
import org.apache.spark.sql.functions._
import scala.collection.Seq

// We disable Broadcast join and AQE, then JOIN on 'make' and 'model'
// In order to see our Skew happening, we need to suppress this behaviour
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled",false)

// Skew eats up 2 Minutes in one of the Stages.  Ouch!!
display(t1DF.join(t2DF, Seq("make", "model"))
.filter(abs(t2DF("engine_size") - t1DF("engine_size")) <= BigDecimal("0.1"))
  .groupBy("registration")
  .agg(avg("sale_price").as("average_price")).collect())

 

//Let AQE and let it figure out the Skew problem and fix it automatically
// First configure the Settings

import org.apache.spark.sql.functions._

// We disable Broadcast join and enable AQE
// In order to see our skew happening, we need to suppress this behaviour
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", true)


spark.conf.set("spark.sql.adaptive.skewedPartitionFactor", 5)
//A partition is considered as skewed if its size in bytes is larger than this threshold and also larger than spark.sql.adaptive.skewJoin.skewedPartitionFactor multiplying the median partition size. Ideally, this config should be set larger than spark.sql.adaptive.advisoryPartitionSizeInBytes.
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","50KB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "10KB")

 

//Solution: Let AQE figure out the Skew problem and fix it automatically

display(t1DF.join(t2DF, Seq("make", "model"))
.filter(abs(t2DF("engine_size") - t1DF("engine_size")) <= BigDecimal("0.1"))
  .groupBy("registration")
  .agg(avg("sale_price").as("average_price")))
