import CaseClasses.EventSchema
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SessionCount {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("kafka_reader-1.0").setMaster("local[*]").setAppName("StreamWordCount")

    val sparkContext = new SparkContext(conf)

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

    stream.foreachRDD{

      rdd =>
        val eventsRdd = rdd.map {
          keyVal =>
            val eventFields = keyVal.value().split(",")
            new EventSchema(eventFields(0).toLong, eventFields(1), eventFields(2).toLong, eventFields(3))
        }
        val numSessions = eventsRdd.groupBy(event => event.sessionID).count
        println("Number of sessions in the last 5 seconds : "+numSessions)
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
