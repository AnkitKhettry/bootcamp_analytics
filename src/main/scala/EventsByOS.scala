import CaseClasses.EventSchema
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object EventsByOS {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("kafka_reader-1.0").setMaster("local[*]").setAppName("StreamWordCount")

    val sparkContext = new SparkContext(conf)

    sparkContext.setLogLevel("ERROR")

    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "event_consumer_group"
    )

    val topicSet = Set("events_topic")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )

    stream.foreachRDD {

      rdd =>
        rdd.map {
          keyVal =>
            val eventFields = keyVal.value().split(",")
            val event = new EventSchema(eventFields(0).toLong, eventFields(1), eventFields(2).toLong, eventFields(3), eventFields(4))
            (event.os, 1)
        }.reduceByKey(_ + _).foreach {
          countForOs =>
            val os = countForOs._1
            val count = countForOs._2
            println("Events for os "+os+" : "+count)
        }
        println
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
