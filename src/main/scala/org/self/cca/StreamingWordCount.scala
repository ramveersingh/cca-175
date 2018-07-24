package org.self.cca

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
/**
  * Created by ramsingh on 6/21/2018.
  */
object StreamingWordCount extends App {
  val conf = new SparkConf().setAppName("spark-streaming").setMaster("yarn-client")
  val ssc = new StreamingContext(conf, Seconds(10))
  val lines = ssc.socketTextStream("localhost", 9999)
  val words = lines.flatMap(line => line.split(" "))
  val wordCount = words.map(word => (word, 1)).reduceByKey((t,v) => t + v)

  wordCount.print()
  ssc.start()
  ssc.awaitTermination()

}
