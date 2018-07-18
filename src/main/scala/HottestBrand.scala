import CaseClasses.EventSchema
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object HottestBrand {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("kafka_reader-1.0").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    sparkContext.setLogLevel("ERROR")

    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "event_consumer_group",
      "auto.offset.reset" -> "earliest"
    )

    val topicSet = Set("events_topic")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )

    stream.foreachRDD {

      rdd =>
        val hottestBrand =
          rdd.map {
            keyVal =>
              val eventFields = keyVal.value().split(",")
              val event = new EventSchema(eventFields(0).toLong, eventFields(1), eventFields(2).toLong, eventFields(3), eventFields(4))
              (event.brand, 1)
          }.reduceByKey(_ + _)
            .reduce {
              (a, b) =>
                if (a._2 > b._2) a else b
            }._1

        println(hottestBrand)
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
