// Databricks notebook source
// # Scala and Spark

// ### Hello World

object HelloWorld{
  def main(args: Array[String]){
    println("Hello, World")
  }
}

//  ### Variables
// val immutable variable 
// var reassigned variable

val pi = 3.14
pi = 3.15
val msg = "hello world"
var x = 1
x = x + 1

// ###Everything is an Object

val temp = 1.+(2)
val temp2 = 1+2

// ### Tuple
// An element in tuple can be accessed using ._index 
// There can be a tuple inside a tuple 

val x = (1,"a",(3.6,3))
println(x._1)
println(x._3._1)
println(x match {case (_,_,(a,_)) => a})

// ### Case Class

// Immutable records
case class R(x: Int,y: String)
val rec = R(1,"a")
println(rec.x)
rec match {case R(_,s) => s}

// Mutable records
case class R (var x: Int, var y: String)
val rec = R(1,"a")
rec.x = 2

// ### Functions

//Anonymous function
(number: Int) => number + 1
() => scala.util.Random.nextInt
(x: Int, y: Int) => (x + 1, y + 1)


//Named function using val, evaluates when defined
val inc = (number: Int) => number + 1
println(inc(1))


//Named function using def, evaluates when called
def inc(number: Int): Int = number + 1
inc(1)


def func(f : Int => Int, v: Int ) = f(v) 
val res1 = func(x => x+1, 3)


val re3 = func(_+1, 3)


//  ## Class


object applyObject {
 def apply(f: Int => Int, v: Int) = f(v)
}

applyObject(x=>x+1,3)
val inc = (x:Int) => x+1

applyObject(inc,3)
applyObject(_+1,3)



// Class
class Complex(real:Double, imaginary:Double){
  def re() = real
  def im() = imaginary
}
// default attribute type is var but can be declared as val



val complexno = new Complex(1.5,2.3)


class Complex(real:Double, imaginary:Double) extends Serializable{
  def re = real
  def im = imaginary
  override def toString() = "" + re + (if (im < 0) "" else "+") + im + "i"
}



val complexno = new Complex(1.5,2.3)


class Complex(real: Double, imaginary: Double) {
  def re() = real
  def im() = imaginary

  // Secondary constructor that takes only the real part
  def this(real: Double) = this(real, 0.0)
  override def toString() = "" + re + (if (im < 0) "" else "+") + im + "i"

}

val complexNumber = new Complex(2.0)


// ## Singleton Object and Class Companion


//Methods and values that are not associated with a class instance belong in singleton objects 
object MyLib{
  def sum(l: List[Int]): Int = l.sum
}
MyLib.sum(List(1,2))


//The equivalent of a static method in Scala is a method in the class companion 
class Rectangle (val length: Int = 0, val width: Int = 0)
object Rectangle{
  def area(r:Rectangle) = r.length * r.width
}

val rect = new Rectangle(10,20)
Rectangle.area(rect)


// ### Pattern Matching using Case Class


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


// ## Collections

val a = Array(1,2,3,4,5,6)
println(a(0))
println(a.length)
val inc = a.map(x => x + 1)
inc.foreach(println)
val inc2 = a.map(_+1)
inc2.foreach(println)
val r = a.reduce((x, y) => x + y)
println(r)
val r2 = a.reduce(_+_)
println(r2)
for(e <- a)
 println(e)
for(i<-0 until a.length)
  a(i) = a(i)+1

val ls = List(1,2,3,4)
println(ls(0))
println(ls.head)
println(ls.last)
val ls2 = ls ++ List(5,6)
ls2.foreach(println)



for(elem <- ls)
  println(elem)
ls.map(_+1)
ls.filter(_%2!=0)



val m = Map(1->"one",3->"three",2->"two")
m(2)
m+(3->"3")


val l = List(1,2,3)
l.reduce((x,y) => x+y)
l.reduce(_+_)



val l = List((1,"a"),(2,"b"),(1,"c"))
l.map(x=>x match { case (i,s) => i+1 })
l.map{case (i,s)=>i+1}
l.map(_._1+1)


// # Spark

val x = sc.textFile("/FileStore/tables/words.txt")
x.collect.foreach(println)
val y2 = x.map(line => line.split(" "))
y2.collect
val y = x.flatMap(line => line.split(" "))
y.collect
val z = y.map(word => (word, 1))
z.collect.foreach(println)
val w = z.reduceByKey(_+_)
w.collect.foreach(println)
