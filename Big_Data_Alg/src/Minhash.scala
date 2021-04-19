import Similarity.remove_punctuation

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math.min
import scala.util.hashing.MurmurHash3

object Minhash{
  def listWords(source: String) : Array[String] ={
    val source_file = Source.fromFile(source, "UTF-8")
    val text = source_file.getLines.mkString(" ").split("\\s+") // any whitespaces
    source_file.close()

    val source_stops_words = Source.fromFile("stopwords_en.txt", "UTF-8")
    val stop_words = source_stops_words.getLines.toList
    source_stops_words.close()

    val no_punctuation = remove_punctuation(text)
    val no_capitals = no_punctuation.mapInPlace(x => x.toLowerCase())
    val filtered = no_capitals.filterNot(stop_words.contains(_))
    filtered
  }


  def makeCharacteristicMatrix(documents : List[String]) : (Array[String], List[Array[Boolean]]) ={
    val wordSets = documents.map(x => listWords(x).toSet)
    val intersection = wordSets.foldLeft(Set[String]())((acc, elem) => acc.union(elem)).toArray
    val columns = wordSets.map(x => {
      val column = new ListBuffer[Boolean]()
      intersection.foreach(y => if (x.contains(y)) column.addOne(true) else column.addOne(false))
      column.toArray
    })
    (intersection, columns)
  }


  def minhash(documents : List[String], SizeOfSignature : Int) : Array[Array[Int]] ={

    val characteristicMatrix = makeCharacteristicMatrix(documents)
    val signatures : Array[Array[Int]] = Array.fill[Array[Int]](documents.length)(Array.fill[Int](SizeOfSignature)(Int.MaxValue))
    val numberOfRows = characteristicMatrix._1.length

    val hashValues = Array.range(0, numberOfRows).map(x => {
      val rowHashes =  Array.fill[Int](SizeOfSignature)(0)
      for (i <- rowHashes.indices){
        rowHashes(i) = MurmurHash3.arrayHash(Array(x), 2*i) % numberOfRows
      }
      rowHashes
    })

    for (i <- 0 until numberOfRows){
      for (j <- documents.indices){
        if (characteristicMatrix._2(j)(i)){
          for (k <- 0 until SizeOfSignature){
            signatures(j)(k) = min(signatures(j)(k), hashValues(i)(k))
          }
        }
      }
    }

    signatures
  }

  def calculateSimilarity(array: Array[Array[Int]]) : ListBuffer[((Int, Int), Double)] ={
    def jaccardEstimator(collection1: Array[Int], collection2 : Array[Int]) : Double ={
      var counter = 0
      for (i <- collection1.indices){
        if (collection1(i) == collection2(i)) counter += 1
      }
      counter.toDouble / collection1.length.toDouble
    }
    val pairs = new ListBuffer[((Int, Int), Double)]()
    for (i <- array.indices){
      for (j <- (i + 1) until array.length){
        pairs.addOne(((i, j), jaccardEstimator(array(i), array(j))))
      }
    }

    pairs
  }

  def jaccardSimilarity(set1: Set[String], set2: Set[String]) : Double ={
    if (set1.isEmpty && set2.isEmpty) 0.0
    else {
      val intersection = set1.intersect(set2).size
      intersection.toDouble / (set1.size + set2.size - intersection).toDouble
    }
  }

  def main (args: Array[String]): Unit = {
    val signatureSize = 250  // Test {50, 100, 250}

    val books = List[String]("./books/Fellowship.txt",
      "./books/Holmes.txt",
      "./books/Iliad.txt",
      "./books/Monte_Christo.txt",
      "./books/Narnia.txt",
      "./books/Odyssey.txt",
      "./books/Peter_Pan.txt",
      "./books/Poirot.txt",
      "./books/Poirot_2.txt",
      "./books/The_Hobbit.txt")

    val signatures = minhash(books, signatureSize)
    val estimatedPairs = calculateSimilarity(signatures)
    val words = books.map(x => listWords(x).toSet)
    val exactPairs = new ListBuffer[((Int, Int), Double)]
    for (i <- words.indices){
      for (j <- (i + 1) until words.length){
        exactPairs.addOne(((i, j), jaccardSimilarity(words(i), words(j))))
      }
    }

    for (i <- estimatedPairs.indices){
      println(estimatedPairs(i)._1._1 + " --- " + estimatedPairs(i)._1._2 + ":      Estimation: " + estimatedPairs(i)._2 + "   Exact: " + exactPairs(i)._2)
    }

  }
}