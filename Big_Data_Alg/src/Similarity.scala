import scala.collection.mutable.ListBuffer
import scala.io.Source

object Similarity {
  def remove_punctuation(text_array : Array[String]): Array[String] = {
    val punctuation = List[String]("“", "”", "’", "\"", "'", ".", ",", ":", ";", "?", "!", "-", "_", "(", ")", "{", "}", "[", "]", ",”")

    def clear_word(word: String): String = {
      var stripped_word = word
      punctuation.foreach(x => {
        stripped_word = stripped_word.stripPrefix(x).stripSuffix(x)
      })
      stripped_word
    }

    text_array.mapInPlace(x => clear_word(x))
  }

  def makeShingles(source: String, k : Int) : (String, Set[String]) ={
    val source_file = Source.fromFile(source, "UTF-8")
    val text = source_file.getLines.mkString(" ").split("\\s+") // any whitespaces
    source_file.close()

    val source_stops_words = Source.fromFile("stopwords_en.txt", "UTF-8")
    val stop_words = source_stops_words.getLines.toList
    source_stops_words.close()

    val no_punctuation = remove_punctuation(text)
    val no_capitals = no_punctuation.mapInPlace(x => x.toLowerCase())
    val filtered = no_capitals.filterNot(stop_words.contains(_))
    val joined = filtered.mkString(" ").toCharArray

    val shinglesList = new ListBuffer[String]()

    for (i <- 0 to (joined.length - k)) {
      shinglesList.addOne(joined.slice(i, i + k).mkString(""))
    }
    (source, shinglesList.toSet)
  }

  def jaccard(text1 : String, text2 : String, k : Int) : Double = {
    val shingles1 = makeShingles(text1, k)._2
    val shingles2 = makeShingles(text2, k)._2

    if (shingles1.isEmpty && shingles2.isEmpty) 0.0
    else {
      val intersection = shingles1.intersect(shingles2).size
      1.0 - (intersection.toDouble / (shingles1.size + shingles2.size - intersection).toDouble)
    }

  }

  def main (args: Array[String]): Unit = {
    val k = 7

    val books = List[String] ("./books/Fellowship.txt",
      "./books/Holmes.txt",
      "./books/Iliad.txt",
      "./books/Monte_Christo.txt",
      "./books/Narnia.txt",
      "./books/Odyssey.txt",
      "./books/Peter_Pan.txt",
      "./books/Poirot.txt",
      "./books/Poirot_2.txt",
      "./books/The_Hobbit.txt")

    for (i <- books.indices){
      for (j <- (i + 1) until books.size){
        println(books(i) + " - " + books(j) + ":    " + jaccard(books(i), books(j), k))
      }
    }

  }
}