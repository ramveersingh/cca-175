package org.self.cca

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume._

/**
  * Created by ramsingh on 6/25/2018.
  */
object FlumeStreamingDepartmentCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Flume Streaming Department Count").setMaster(args(0))
    val ssc = new StreamingContext(conf, Seconds(30))

    val stream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, args(1), args(2).toInt)
    val messages: DStream[String] = stream.map(s => new String(s.event.getBody.array()))

    val departmentMessages = messages.filter(m => {
      val endpoint = m.split(" ")(6)
      endpoint.split("/")(1) == "department"
    })

    val departments: DStream[(String, Int)] = departmentMessages.map { dm =>
      val endpoint = dm.split(" ")(6)
      (endpoint.split("/")(2), 1)
    }

    val departmentTraffic: DStream[(String, Int)] = departments.reduceByKey((total, value) => total + value)

    departmentTraffic.print()

    departmentTraffic.saveAsTextFiles("/user/cloudera/flume-demo/dept-wise-traffic")

    ssc.start()
    ssc.awaitTermination()
  }
}
