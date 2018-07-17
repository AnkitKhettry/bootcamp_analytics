import java.util.Properties

import CaseClasses.EventSchema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaWriter {

  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")


    KafkaZKUtils.startZookeeper
    KafkaZKUtils.startKafka

    val topic = "events_topic"

    val producer = new KafkaProducer[String, String](props)


    //pushes ten events every second for 10 minutes
    (1 to 6000).foreach  {
      idx =>
        val record = new ProducerRecord(topic, "key"+idx, randomEvent.toString)
        producer.send(record)
        Thread.sleep(100)
    }

    producer.close()
  }

  def randomEvent() = {

    val eventTypes = List("pdp_view", "screen_load", "app_launch", "add_to_collection", "add_to_cart")
    val brands = List("nike", "adidas", "puma", "roadster")

    new EventSchema(
      System.currentTimeMillis(),
      eventTypes((Math.random()*5).toInt),      //one of the possible 5 event types
      (Math.random()*100).toLong,               //session ID between 0 and 99
      brands((Math.random()*4).toInt)           //one of the 4 brands
    )
  }
}
