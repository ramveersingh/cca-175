package org.self.cca

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * Created by ramsingh on 6/23/2018.
  */
object KafkaStreamingDepartmentCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDeptWiseTraffic").setMaster(args(0))
    val ssc = new StreamingContext(conf, Seconds(30))

    val messages: ReceiverInputDStream[String] = ssc.socketTextStream(args(1), args(2).toInt)

    val departmentMessages: DStream[String] = messages.filter(message =>  {
      val endpoint = message.split(" ")(6)
      endpoint.split("/")(1) == "department"
    })

    val departments: DStream[(String, Int)] = departmentMessages.map { dm =>
      val endpoint = dm.split(" ")(6)
      (endpoint.split("/")(2), 1)
    }

    val departmentTraffic: DStream[(String, Int)] = departments.reduceByKey((total, value) => total + value)

    departmentTraffic.print()

    departmentTraffic.saveAsTextFiles("/user/cloudera/streaming/dept-wise-traffic")

    ssc.start()
    ssc.awaitTermination()
  }

}
