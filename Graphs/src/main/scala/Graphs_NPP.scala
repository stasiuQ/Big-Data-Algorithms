import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import java.io.{BufferedWriter, File, FileWriter}


object Graphs_NPP {

  def calculateClusteringCoefficient (numberOfNodes : Int, edges : RDD[(Int, Int)]): Double ={  // using NodeItr++ algorithm

    def edgeMaker (node : (Int, Set[Int])) : List[(Int, Int)] ={
      val tempList = new ListBuffer[(Int, Int)]()
      node._2.foreach(x => tempList.+=:((node._1, x)))
      tempList.toList
    }

    val undirectedNodes = edges.map(x => (x._2, x._1)).++(edges).groupBy(x => x._1).map(x => (x._1, x._2.toSet.map((y: (Int, Int)) => y._2)))
    val degrees = undirectedNodes.sortByKey().map(x => x._2.size).collect()  // caching degrees into RAM memory - performance boost

    val undirectedEdges = undirectedNodes.flatMap(edgeMaker).map(x => if (x._1 > x._2) (x._2, x._1) else (x._1, x._2)).distinct() // starting map1 procedure, sorting edges like (1,2) ascending
    val mapper1 = undirectedEdges.map(x => if (degrees(x._1 - 1) > degrees(x._2 - 1)) (x._2, x._1) else (x._1, x._2))  // setting higher degree at a first index

    val reducer1 = mapper1.groupBy(x => x._1).map(x => (x._1, x._2.map(y => y._2).toList.sorted)).flatMap(x => {
      val tempList = new ListBuffer[((Int, Int), Int)]()
      val listSize = x._2.length
      for (i <- 0 until listSize){
        for (j <- i + 1 until listSize ){  // looping over all sorted pairs
          tempList.+=:(((x._2(i), x._2(j)), x._1))
        }
      }
      tempList.toList
    })

    val mapper2: RDD[((Int, Int), (Int, String))] = reducer1.join(undirectedEdges.map(x => ((x._1, x._2), "x")))
    val reducer2 = mapper2.flatMap(x => Array((x._1._1, 1.0), (x._1._2, 1.0), (x._2._1, 1.0))).reduceByKey(_ + _)
      .map(x => {
        if (degrees(x._1 - 1) > 1) {
          (x._1, (2.0 * x._2) / (degrees(x._1 - 1) * (degrees(x._1 - 1) - 1).toDouble))
        }
        else {
          (x._1, 0.0)
        }

      })


    val averageClusteringCoefficient = reducer2.map(x => x._2).fold(0.0)((x, y) => x + y) / numberOfNodes.toDouble
    averageClusteringCoefficient
  }

  def main(args : Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.INFO)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Graphs")
      .set("spark.local.dir", "C:\\WORKING\\BDA\\BDAlg\\Spark_TMP")

    val inputFileName = "web-Stanford.txt"
    val sc = new SparkContext(conf)
    val lines = sc.textFile(inputFileName)

    val numberOfNodes = 281903
    val edges = lines.map(line => (line.split("\\s+")(0).toInt, line.split("\\s+")(1).toInt))
    val outDegree = edges.groupBy(x => x._1).map(x => (x._1, x._2.size)) // Reducer
    val inDegree = edges.groupBy(x => x._2).map(x => (x._1, x._2.size))

    val averageOutDegree = outDegree.map(x => x._2).fold(0)((x, y) => x + y).toDouble / numberOfNodes.toDouble
    val averageInDegree = inDegree.map(x => x._2).fold(0)((x, y) => x + y).toDouble / numberOfNodes.toDouble
    val averageClusteringCoefficient = calculateClusteringCoefficient(numberOfNodes, edges)

    /* Saving results to the output file */
    val file = new File("output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Average out degree: " + averageOutDegree.toString + "\n")
    bw.write("Average in degree:  " + averageInDegree.toString + "\n")
    bw.write("Average clustering coefficient: " + averageClusteringCoefficient.toString + "\n")
    bw.close()
  }
}

