import java.util.Properties
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.json4s._
import org.json4s.native.Serialization._
import org.json4s.native.Serialization
import org.json4s.native.Json
import org.json4s.DefaultFormats
import scala.io.Source


/**
 * Create by loverMonkey
 */
object GitOperKafka {
  def main(args: Array[String]): Unit = {
    val kafkaProp = new Properties()
    kafkaProp.put("bootstrap.servers", "47.103.202.001:9092")
    kafkaProp.put("acks", "all")
    kafkaProp.put("retries", "0")
    //kafkaProp.put("batch.size", 16384)//16k
    kafkaProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //定义字段类型
    val column_names = Array("a","b","c","d","e","f","g","h","i","j","k","l","m","n","o")
    val producer = new KafkaProducer[String, String](kafkaProp)
    val lines = Source.fromFile("D:\\scalaScript\\scala1\\src\\main\\scala\\test.txt").getLines()
    while (lines.hasNext) {
      val line = lines.next()
      //文件分割符
      val contents = line.split('|')
      //将字段名和value转为tuple
      val con_map = column_names.zip(contents)
      //将tuple转为map，然后转为json
      implicit val formats = Serialization.formats(NoTypeHints)
      val res =  write(con_map.toMap)
      //向topic推送数据
      val record = new ProducerRecord[String, String]("test111",res)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (metadata != null) {
            println("发送成功")
            println(metadata)
          }
          if (exception != null) {
            println("消息发送失败")
            println(exception)
          }
        }
      })
    }
    producer.close()
  }
}
