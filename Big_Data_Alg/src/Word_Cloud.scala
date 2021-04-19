import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Word_Cloud {
  def remove_punctuation(text_array : Array[String]): Array[String] = {
    val punctuation  = List[String](".",",","'",":",";","?", "!", "-", "(", ")")
    def clear_word(word:String) : String = {
      var stripped_word = word
      punctuation.foreach(x => {stripped_word = stripped_word.stripPrefix(x).stripSuffix(x)})
      stripped_word
    }
    text_array.mapInPlace(x => clear_word(x))
  }
  def make_word_cloud(filename: String): Unit = {
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

    // FileWriter
    val file = new File("output.csv")
    val bw = new BufferedWriter(new FileWriter(file))

    for (word <- sorted) {
      if (word._1 != "") {
        bw.write(word._2.toString + ";" + word._1)
        bw.newLine()
      }
    }

    bw.close()
  }

  def main(args: Array[String]): Unit = {
    make_word_cloud("Fellowship.txt")
  }
}
