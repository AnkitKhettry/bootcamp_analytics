import java.io.File
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.zookeeper.server.ZooKeeperServerMain

object KafkaZKUtils {

  val BROKER_HOST = "127.0.0.1"
  val BROKER_PORT = "9092"
  val ZK_PORT = "2181"
  val nodeId = "0"
  val zkConnect: String = "localhost:" + ZK_PORT

  def startZookeeper(): Unit = {

    val zkTmpDir = File.createTempFile("zookeeper", "test")
    zkTmpDir.delete()
    zkTmpDir.mkdir()

    new Thread(new Runnable {
      override def run(): Unit = {
        ZooKeeperServerMain.main(Array[String](
          ZK_PORT, zkTmpDir.getAbsolutePath))
      }
    }).start()

    try {
      Thread.sleep(1000)
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }

  }

  def startKafka(): Unit = {

    val kafkaDir = File.createTempFile("kafka", "test")
    val props = new Properties()

    kafkaDir.delete()
    kafkaDir.mkdir()

    props.put("broker.id", nodeId)
    props.put("port", BROKER_PORT)
    props.put("zookeeper.connect", zkConnect)
    props.put("host.name", BROKER_HOST)
    props.put("auto.create.topics.enable", "true")
    props.put("log.dir", kafkaDir.getAbsolutePath)

    val conf = new KafkaConfig(props)
    val server = new KafkaServerStartable(conf)

    server.startup()

    try {
      Thread.sleep(1000)
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }
  }
}