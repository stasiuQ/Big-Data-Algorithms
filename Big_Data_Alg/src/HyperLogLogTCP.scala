import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math.{log, log10, max, pow}
import scala.util.hashing.MurmurHash3


class HyperLogLog (m : Int, hashSize : Int, extended : Boolean){
  val alpha : Double = {
    if (m == 16)
      0.673
    else if (m == 32)
      0.690
    else if (m == 64)
      0.709
    else
      0.7213 / (1.0 + 1.079 / m)
  }
  var counters : Array[Int] = Array.fill[Int](m)(0)
  if (!extended) counters = Array.fill[Int](m)(Int.MinValue)
  val M : Int = (log10(m.toDouble)/log10(2.0)).toInt

  def fromBinary(expression : String) : Int ={
    var number = 0
    val charArray = expression.toCharArray.reverse

    for (i <- charArray.indices){
      if (charArray(i) == '1'){
        number += pow(2.0, i.toDouble).toInt
      }
    }
    number
  }

  def rho (expression: String) : Int ={
    val charArray = expression.toCharArray
    for (i <- charArray.indices){
      if (charArray(i) == '1') return i + 1
    }
    hashSize - M + 1
  }

  def calculateHash (element : String) : String ={
    val numberOfHashes = hashSize / 32
    val hashes = new ListBuffer[String]()
    for (i <- 0 until numberOfHashes){
      val bareHash = MurmurHash3.arrayHash(element.toCharArray, i).toBinaryString
      val hash = bareHash.reverse.padTo(32, '0').reverse
      hashes.+=(hash)
    }
    hashes.foldLeft("")(_++_)

  }
  def updateCounters (stream : List[String]) : Unit ={
    stream.foreach(x => {
      val hash = calculateHash(x)
      val index = fromBinary(hash.take(M))
      counters(index) = max(counters(index), rho(hash.takeRight(hashSize - M)))
    })
  }

  def estimateCardinality() : Double ={
    val Z = (m * m).toDouble / counters.foldLeft(0.0)((acc, elem) => acc + pow(0.5, elem.toDouble))
    val estimator = alpha * Z
    if (extended) rangeCorrections(estimator)
    else estimator
  }

  def rangeCorrections(estimation : Double) : Double ={
    if (estimation <= (5.0/2.0) * m){  // small range correction
      val V = counters.foldLeft(0)((acc, elem) => if (elem == 0) acc + 1 else acc)  // the number of registers equals to 0
      if (V != 0) m.toDouble * log(m.toDouble / V.toDouble)
      else estimation
    }
    else if (estimation <= (1.0 / 30.0) * pow(2.0, 32.0)){  // intermediate range - no corrections
      estimation
    }
    else {  // large range corrections
      -pow(2.0, 32.0) * log(1.0 - (estimation / pow(2.0, 32.0)))
    }
  }

}

object HyperLogLogTCP {
  def calculate(stream : List[String]) : Int ={
    val table = new ListBuffer[String]()
    stream.foreach(x => {if (!table.contains(x)) table.addOne(x)})
    table.length
  }

  def main(args: Array[String]): Unit = {

    val sourceCounter = new HyperLogLog(256, 128, true)
    val destinationCounter = new HyperLogLog(256, 128, true)
    val pairsCounter = new HyperLogLog(256, 128, true)

    val source_file = Source.fromFile("lbl-pkt-4.tcp", "UTF-8")

    val records = source_file.getLines().map(x => (x.split("\\s+")(1), x.split("\\s+")(2))).toList   // any whitespaces
    source_file.close()

    val sources = records.map(x => x._1)
    val destinations = records.map(x => x._2)
    val pairs = records.map(x => if (x._1.toInt > x._2.toInt) x._2 + "-" + x._1 else x._1 + "-" + x._2 )

    sourceCounter.updateCounters(sources)
    destinationCounter.updateCounters(destinations)
    pairsCounter.updateCounters(pairs)

    println("Sources estimation: " + sourceCounter.estimateCardinality().toString)
    println("Exact sources: " + calculate(sources).toString)

    println("Destinations estimation: " + destinationCounter.estimateCardinality().toString)
    println("Exact destinations: " + calculate(destinations).toString)

    println("Pairs estimation: " + pairsCounter.estimateCardinality().toString)
    println("Exact pairs: " + calculate(pairs).toString)

  }
}