import scala.util.hashing.MurmurHash3

abstract class Hashable() {
  val id : String
}

class Car (name: String, capacity : Int, year : Int) extends Hashable(){

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

class ExtendedFlajoletAlgorithm (size : Int, numberOfHashFun : Int) {
  val phi = 0.77351
  var bitmap : Array[Int]= Array.fill[Int](size)(0)

  def fillBuffer (stream : LazyList[Hashable], hashNumber : Int): Unit = {
    stream.foreach{
      x => {
        val id_array = x.id.toCharArray
        val index = rho(MurmurHash3.arrayHash(id_array, hashNumber).abs % math.pow(2, size).toInt)

        bitmap(index) = 1
      }
    }
  }

  def cardinality (): Double = {
    var i = 0
    var R = -1
    while (R == -1 && i < size){
      if (bitmap(i) == 0){
        R = i
      }
      i += 1
    }
    math.pow(2, R)/phi
  }

  def rho (hash : Int): Int = {
    val binary = hash.toBinaryString.reverse
    for (i <- 0 until binary.length) {
      if ( (binary(i).toInt - 48) == 1 ){
        return i
      }
    }
    size
  }

  def estimateCardinality(stream : LazyList[Hashable]): Long = {
    var cardList = Array[Double]()

    for (i <- 1 to numberOfHashFun){
      fillBuffer(stream, i)
      cardList = cardList :+ cardinality()
      bitmap = Array.fill[Int](size)(0)
    }
    val (lower, upper) = cardList.sortWith(_<_).splitAt(cardList.length / 2)
    if (cardList.length % 2 != 0) {
      upper.head.round
    } else {
      ((lower.last + upper.head) / 2.0).round
    }


  }
}

object Flajolet {
  def main(args: Array[String]): Unit = {
    val car1 = new Car("Opel Astra", 1600, 1997)
    val car2 = new Car("Mazda RX7", 1300, 1992)
    val car3 = new Car("Dodge Charger", 4800, 2012)
    val car4 = new Car("Peugeot 206", 1200, 2000)
    val car5 = new Car("Mazda 6", 2200, 2019)

    val data_stream = car5 #:: car4 #:: car1 #:: car2 #:: car3 #:: car2 #:: car3 #:: car2 #:: car1 #:: car2 #:: car2 #:: car1 #:: car3 #:: car3 #:: car1 #:: car1 #:: car3 #:: LazyList.empty

    val test = new ExtendedFlajoletAlgorithm(8, 10)
    println("Estimated cardinality: " + test.estimateCardinality(data_stream).toString)

  }
}