import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object list1_books {
  def remove_punctuation(text_array : Array[String]): Array[String] = {
    val punctuation  = List[String]("“", "”", "’", "\"", "'", ".", ",", ":", ";", "?", "!", "-", "_", "(", ")", "{", "}", "[", "]", ",”")
    def clear_word(word:String) : String = {
      var stripped_word = word
      punctuation.foreach(x => {stripped_word = stripped_word.stripPrefix(x).stripSuffix(x)})
      stripped_word
    }
    text_array.mapInPlace(x => clear_word(x))
  }
  def word_count (filename: String): (String, Seq[(String, Int)]) = {
    val source_file = Source.fromFile(filename, "UTF-8")
    val text = source_file.getLines.mkString(" ").split("\\s+") // any whitespaces
    source_file.close()

    val source_stops_words = Source.fromFile("stopwords_en.txt", "UTF-8")
    val stop_words = source_stops_words.getLines.toList
    source_stops_words.close()

    val no_punctuation = remove_punctuation(text)
    val no_capitals = no_punctuation.mapInPlace(x => x.toLowerCase())
    val filtered = no_capitals.filterNot(stop_words.contains(_))

    val counters = new ListBuffer[(String, Int)]()
    filtered.foreach(x => counters.addOne(x, 1))

    val grouped = counters.groupBy(x => x._1)
    val reduced = grouped.map(x => (x._1, x._2.length))
    val sorted = reduced.toSeq.sortWith((x, y) => x._2 > y._2)
    (filename, sorted)

  }

  def TF_IDF (books_words: List[(String, Seq[(String, Int)])]) : List[(String, Seq[(String, Float)])] = {
    def IDF (word: String): Int = {
      val counters = new ListBuffer[Int]()
      books_words.foreach(x => {
        if (x._2.map(x => x._1).contains(word)) {
          counters.addOne(1)
        }
        else
          counters.addOne(0)
      })
      books_words.length / counters.sum
    }
    books_words.map(x => (x._1, {
      //val all_terms = x._2.reduce((p,q) => ("", p._2 + q._2))._2
      val max_word = x._2.head._2
      x._2.map(y => (y._1, IDF(y._1) * y._2.toFloat / max_word))
    }.sortWith((x, y) => x._2 > y._2)))
  }

  def list_by_TF_IDF (books: List[(String, Seq[(String, Float)])], word: String) : List[(String, Float)] = {
    val TF_IDF_books = books.map(x => (x._1, {x._2.find(y => y._1 == word )}))
    TF_IDF_books.map(x => (x._1, { x._2 match {
      case Some(z) => z._2
      case None    => 0
    }})).filterNot(x => x._2 == 0).sortWith((x, y) => x._2 > y._2)
  }

  def main (args: Array[String]): Unit = {
    val words = List[(String, Seq[(String, Int)])] (word_count("./books/Fellowship.txt"),
      word_count("./books/Holmes.txt"),
      word_count("./books/Iliad.txt"),
      word_count("./books/Monte_Christo.txt"),
      word_count("./books/Narnia.txt"),
      word_count("./books/Odyssey.txt"),
      word_count("./books/Peter_Pan.txt"),
      word_count("./books/Poirot.txt"),
      word_count("./books/Poirot_2.txt"),
      word_count("./books/The_Hobbit.txt"))

    val tf_idf_books = TF_IDF(words)

    // FileWriter
    val file = new File("output.csv")
    val bw = new BufferedWriter(new FileWriter(file))

    words.foreach(x => {bw.write(x._1 + " : " + x._2.length.toString + " words" + "\n")})
    bw.newLine()

    for (book <- tf_idf_books) {
      bw.write(book._1 + "\n")
      for (word <- book._2.take(20)){
        bw.write(word._2.toString + ";" + word._1 + "\n")
      }
      bw.newLine()
    }

    bw.close()

    println(list_by_TF_IDF(tf_idf_books, "gandalf"))
    println(list_by_TF_IDF(tf_idf_books, "sea"))
    println(list_by_TF_IDF(tf_idf_books, "bronthonosaur"))
    println(list_by_TF_IDF(tf_idf_books, "book"))
    println(list_by_TF_IDF(tf_idf_books, "sword"))
    println(list_by_TF_IDF(tf_idf_books, "crime"))
    println(list_by_TF_IDF(tf_idf_books, "castle"))
  }

}
