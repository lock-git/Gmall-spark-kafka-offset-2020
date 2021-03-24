package com.atguigu.realtime.ads

import java.time.LocalDateTime

import com.atguigu.realtime.BaseAppV4
import com.atguigu.realtime.bean.OrderWide
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/16 10:42
 */
object AdsOrderWideApp extends BaseAppV4 {
    // 从kafka消费数据, 偏移量在mysql
    override val groupId: String = "AdsOrderWideApp"
    override val topic: String = "dws_order_wide"
    override val master: String = "local[2]"
    override val appName: String = "AdsOrderWideApp"
    override val batchTime: Int = 5
    
    override def run(ssc: StreamingContext,
                     sourceStream: DStream[String],
                     offsetRanges: ListBuffer[OffsetRange]): Unit = {
        // scalalike jdbc 去初始化配置: 加载我们的默认配置文件
        DBs.setup()
        sourceStream
            .map(json => {
                implicit val f = org.json4s.DefaultFormats
                val orderWide = JsonMethods.parse(json).extract[OrderWide]
                (orderWide.tm_id, orderWide.tm_name) -> BigDecimal(orderWide.final_detail_amount)
            })
            .reduceByKey(_ + _)
            .foreachRDD((rdd: RDD[((Long, String), BigDecimal)]) => {
                
                // 1. 先把数据取出来
                val time: LocalDateTime = LocalDateTime.now()
                val statTime = s"${time.toLocalDate} ${time.toLocalTime.toString.substring(0, 8)}"
                val data = rdd.collect().map {
                    case ((tmId, tmName), amount) =>
                        List(statTime, tmId, tmName, amount)
                }
                println("data: " + data.mkString(", "))
                // 2. offset数据也要拿到
                val offsets = offsetRanges.map(offsetRange => {
                    List(groupId, topic, offsetRange.partition, offsetRange.untilOffset)
                })
                
                DB.localTx(implicit session => {
                    // 这里执行的代码k,都会在同一个事务中...
                    val dataSql =
                        """
                          |REPLACE into tm_amount values(?, ?, ?, ?)
                          |""".stripMargin
                    val offsetSql =
                        """
                          |REPLACE into ads_offset values(?, ?, ?, ?)
                          |""".stripMargin
                    
                    SQL(dataSql).batch(data: _*).apply()
                    SQL(offsetSql).batch(offsets: _*).apply()
                })
                
            })
    }
}

/*



 */
