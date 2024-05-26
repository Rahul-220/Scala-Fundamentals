// Databricks notebook source
// MAGIC %md
// MAGIC # Scala and Spark

// COMMAND ----------

// MAGIC %md
// MAGIC ### Hello World

// COMMAND ----------

object HelloWorld{
  def main(args: Array[String]){
    println("Hello, World")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Variables
// MAGIC val immutable variable <br>
// MAGIC var reassigned variable

// COMMAND ----------

val pi = 3.14
pi = 3.15
val msg = "hello world"
var x = 1
x = x + 1

// COMMAND ----------

// MAGIC %md
// MAGIC ###Everything is an Object

// COMMAND ----------

val temp = 1.+(2)
val temp2 = 1+2

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tuple
// MAGIC An element in tuple can be accessed using ._index <br/>
// MAGIC There can be a tuple inside a tuple <br/>

// COMMAND ----------

val x = (1,"a",(3.6,3))
println(x._1)
println(x._3._1)
println(x match {case (_,_,(a,_)) => a})

// COMMAND ----------

// MAGIC %md
// MAGIC ### Case Class

// COMMAND ----------

// Immutable records
case class R(x: Int,y: String)
val rec = R(1,"a")
println(rec.x)
rec match {case R(_,s) => s}

// COMMAND ----------

// Mutable records
case class R (var x: Int, var y: String)
val rec = R(1,"a")
rec.x = 2

// COMMAND ----------

// MAGIC %md
// MAGIC ### Functions

// COMMAND ----------

//Anonymous function
(number: Int) => number + 1
() => scala.util.Random.nextInt
(x: Int, y: Int) => (x + 1, y + 1)

// COMMAND ----------

//Named function using val, evaluates when defined
val inc = (number: Int) => number + 1
println(inc(1))

// COMMAND ----------

//Named function using def, evaluates when called
def inc(number: Int): Int = number + 1
inc(1)

// COMMAND ----------

def func(f : Int => Int, v: Int ) = f(v) 
val res1 = func(x => x+1, 3)

// COMMAND ----------

val re3 = func(_+1, 3)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Class

// COMMAND ----------

object applyObject {
 def apply(f: Int => Int, v: Int) = f(v)
}

applyObject(x=>x+1,3)
val inc = (x:Int) => x+1

applyObject(inc,3)
applyObject(_+1,3)


// COMMAND ----------

// Class
class Complex(real:Double, imaginary:Double){
  def re() = real
  def im() = imaginary
}
// default attribute type is var but can be declared as val

// COMMAND ----------

val complexno = new Complex(1.5,2.3)

// COMMAND ----------

class Complex(real:Double, imaginary:Double) extends Serializable{
  def re = real
  def im = imaginary
  override def toString() = "" + re + (if (im < 0) "" else "+") + im + "i"
}

// COMMAND ----------

val complexno = new Complex(1.5,2.3)

// COMMAND ----------

class Complex(real: Double, imaginary: Double) {
  def re() = real
  def im() = imaginary

  // Secondary constructor that takes only the real part
  def this(real: Double) = this(real, 0.0)
  override def toString() = "" + re + (if (im < 0) "" else "+") + im + "i"

}

val complexNumber = new Complex(2.0)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Singleton Object and Class Companion

// COMMAND ----------

//Methods and values that are not associated with a class instance belong in singleton objects 
object MyLib{
  def sum(l: List[Int]): Int = l.sum
}
MyLib.sum(List(1,2))

// COMMAND ----------

//The equivalent of a static method in Scala is a method in the class companion 
class Rectangle (val length: Int = 0, val width: Int = 0)
object Rectangle{
  def area(r:Rectangle) = r.length * r.width
}

val rect = new Rectangle(10,20)
Rectangle.area(rect)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pattern Matching using Case Class

// COMMAND ----------

case class Person(name: String, age: Int)

def describePerson(person: Person): String = person match {
  case Person(name, age) if age < 18 => s"$name is a minor."
  case Person(name, age) if age < 65 => s"$name is an adult."
  case Person(name, age)            => s"$name is a senior citizen."
  case _ => "None"
}

val alice = Person("Alice", 25)
val bob = Person("Bob", 70)

println(describePerson(alice)) 
println(describePerson(bob))


// COMMAND ----------

// MAGIC %md
// MAGIC ## Collections

// COMMAND ----------

val a = Array(1,2,3,4,5,6)
//println(a(0))
//println(a.length)
//val inc = a.map(x => x + 1)
//inc.foreach(println)
//val inc2 = a.map(_+1)
//inc2.foreach(println)
//val r = a.reduce((x, y) => x + y)
//println(r)
//val r2 = a.reduce(_+_)
//println(r2)
for(e <- a)
 println(e)
for(i<-0 until a.length)
  a(i) = a(i)+1

// COMMAND ----------

val ls = List(1,2,3,4)
//println(ls(0))
//println(ls.head)
//println(ls.last)
val ls2 = ls ++ List(5,6)
ls2.foreach(println)

// COMMAND ----------

for(elem <- ls)
  println(elem)
ls.map(_+1)
ls.filter(_%2!=0)

// COMMAND ----------

val m = Map(1->"one",3->"three",2->"two")
m(2)
m+(3->"3")

// COMMAND ----------

val l = List(1,2,3)
l.reduce((x,y) => x+y)
l.reduce(_+_)

// COMMAND ----------

val l = List((1,"a"),(2,"b"),(1,"c"))
l.map(x=>x match { case (i,s) => i+1 })
l.map{case (i,s)=>i+1}
l.map(_._1+1)

// COMMAND ----------

// MAGIC %md
// MAGIC # Spark

// COMMAND ----------

val x = sc.textFile("/FileStore/tables/words.txt")
x.collect.foreach(println)
//val y2 = x.map(line => line.split(" "))
//y2.collect
val y = x.flatMap(line => line.split(" "))
y.collect
//val z = y.map(word => (word, 1))
//z.collect.foreach(println)
//val w = z.reduceByKey(_+_)
//w.collect.foreach(println)
