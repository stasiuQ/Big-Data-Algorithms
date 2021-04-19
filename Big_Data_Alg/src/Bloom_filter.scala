import scala.util.hashing.MurmurHash3

abstract class Filterable () {
  val id : String
}


class Car (name: String, capacity : Int, year : Int) extends Filterable(){

  val carName : String = name
  val carCapacity : Int = capacity
  val carYear : Int = year
  val id : String = name + capacity.toString + year.toString

  def == (that: Car): Boolean = {
    if ((carName == that.carName) && (carCapacity == that.carCapacity) && (carYear == that.carYear)){
      true
    }
    else{
      false
    }
  }
}

class  Filter (size:Int) {
  var buffer : Array[Int]= Array.fill[Int](size)(0)

  def printArray (): Unit = {
    for (i <- 0 until size) {
      println(buffer(i))
    }
    println("")
  }

  def add (stream : LazyList[Filterable]): Unit = {
    stream.foreach{
      x => {
        val id_array = x.id.toCharArray
        buffer(MurmurHash3.arrayHash(id_array).abs % size) += 1
        buffer(MurmurHash3.arrayHash(id_array, 5).abs % size) += 1
        buffer(MurmurHash3.arrayHash(id_array.reverse).abs % size) += 1

      }
    }
  }

  def exists (element : Filterable): Boolean = {
    val id_array = element.id.toCharArray
    val cond1 = buffer(MurmurHash3.arrayHash(id_array).abs % size) == 0
    val cond2 = buffer(MurmurHash3.arrayHash(id_array, 5).abs % size) == 0
    val cond3 = buffer(MurmurHash3.arrayHash(id_array.reverse).abs % size) == 0

    if (cond1 || cond2 || cond3) {
      false
    }
    else {
      true
    }
  }

  def remove (stream : LazyList[Filterable]): Unit = {
    stream.foreach{
      x => {
        val id_array = x.id.toCharArray
        if (exists(x)) {
          buffer(MurmurHash3.arrayHash(id_array).abs % size) -= 1
          buffer(MurmurHash3.arrayHash(id_array, 5).abs % size) -= 1
          buffer(MurmurHash3.arrayHash(id_array.reverse).abs % size) -= 1
        }
        else {
          println(x.id + " : Cannot remove a non-existing element!")
        }

      }
    }
  }
  
}

object Bloom_filter {
  def main (args : Array[String]) : Unit = {
    val car1 = new Car("Opel Astra", 1600, 1997)
    val car2 = new Car("Mazda RX7", 1300, 1992)
    val car3 = new Car("Dodge Charger", 4800, 2012)
    val car6 = new Car("Peugeot 206", 1200, 2000)
    val car7 = new Car("Mazda 6", 2200, 2019)
    val car8 = new Car("VW Golf", 1600, 1998)

    val data_stream = car1 #:: car2 #:: car3 #:: car2 #:: car6 #:: car7 #:: car7 #:: car2 #:: car2 #:: car1 #:: car3 #:: car3 #:: car1 #:: car1 #:: car3 #:: LazyList.empty

    val my_Bloom_filter = new Filter(15)
    my_Bloom_filter.add(data_stream)

    println( "Car1: " + my_Bloom_filter.exists(car1).toString)
    println( "Car2: " + my_Bloom_filter.exists(car2).toString)
    println( "Car3: " + my_Bloom_filter.exists(car3).toString)
    println( "Car6: " + my_Bloom_filter.exists(car6).toString)
    println( "Car7: " + my_Bloom_filter.exists(car7).toString)
    println( "Car8: " + my_Bloom_filter.exists(car8).toString)

    println("Removing car number 6.")
    my_Bloom_filter.remove(car6#::LazyList.empty)

    println( "Car6: " + my_Bloom_filter.exists(car6).toString)

  }
}