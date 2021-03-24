package com.atguigu.realtime.ods

import com.atguigu.realtime.BaseAppV1
import com.atguigu.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/9 9:31
 * // 从 topic: gmall_db_canal 读数据, 分流, 写入到ods层: 不同的topic中
 */
object OdsCanalApp extends BaseAppV1 {
    override val groupId: String = "OdsCanalApp"
    override val topic: String = "gmall_db_canal"
    override val master: String = "local[2]"
    override val appName: String = "OdsCanalApp"
    override val batchTime: Int = 3
    
    val tables = List(
        "order_info",
        "order_detail",
        "user_info",
        "base_province",
        "base_category3",
        "sku_info",
        "spu_info",
        "base_trademark")
    
    
    override def run(ssc: StreamingContext,
                     sourceStream: DStream[String],
                     offsetRanges: ListBuffer[OffsetRange]) = {
        sourceStream
            .flatMap(line => {
                implicit val f = org.json4s.DefaultFormats
                
                val lineJValue: JValue = JsonMethods.parse(line)
                val dataJValue: JValue = lineJValue \ "data"
                val operateType = (lineJValue \ "type").extract[String]
                val tableName = (lineJValue \ "table").extract[String]
                dataJValue
                    .children
                    .map(child => (tableName.toLowerCase(), operateType.toLowerCase(), Serialization.write(child)))
            })
            .filter {
                case (tableName, operateType, data) =>
                    tables.contains(tableName) && (operateType == "insert" || operateType == "update")
            }
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    // 创建kafka生产者
                    val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer
                    it.foreach {
                        case (tableName, operateType, data) =>
                            println(data)
                            // 写入每条数据  order_info 这个表只插入的数据, 修改的订单状态的数据不写
                            if (tableName != "order_info") {
                                producer.send(new ProducerRecord[String, String](s"ods_${tableName}", data))
                            } else if (operateType == "insert") {
                                producer.send(new ProducerRecord[String, String](s"ods_${tableName}", data))
                            }
                    }
                    // 关闭生产者
                    producer.close()
                    
                })
                saveOffsets(offsetRanges) // 保存偏移量
            })
        
    }
}
