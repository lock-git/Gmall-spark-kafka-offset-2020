package com.atguigu.realtime.app

import java.time.LocalDate

import com.atguigu.realtime.BaseAppV1
import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.util.{MyESUtil, MyRedisUtil}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.JValue
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/7 16:36
 */
object DauApp_2 extends BaseAppV1 {
    override val groupId: String = "DauApp_2"
    override val topic: String = "gmall_start_topic"
    override val master: String = "local[2]"
    override val appName: String = "DauApp_2"
    override val batchTime: Int = 5
    
    override def run(ssc: StreamingContext,
                     sourceStream: DStream[String],
                     offsetRanges: ListBuffer[OffsetRange]): Unit = {
        
        val startupLogStream: DStream[StartupLog] = parseToStartupLog(sourceStream)
        val result: DStream[StartupLog] = distinct_2(startupLogStream)
        
        result.foreachRDD(rdd => {
            rdd.cache()
            println("-------------------------")
            println(s"Time: ${System.currentTimeMillis()}")
            rdd.collect().foreach(x => println("纯手工: " + x))
            println("-------------------------")
            rdd.foreachPartition((it: Iterator[StartupLog]) => {
                val today: String = LocalDate.now().toString
                MyESUtil.insertBulk(s"gmall_dau_info_$today", it.map(log => (log.mid, log)))
            })
            //OffsetsManager.saveOffsets(groupId, topic, offsetRanges)
            saveOffsets(offsetRanges)
        })
    }
    
    
    def parseToStartupLog(sourceStream: DStream[String]) = {
        sourceStream.map(record => {
            // Gson jackson
            val j: JValue = JsonMethods.parse(record)
            val common: JValue = j \ "common"
            val ts = j \ "ts"
            implicit val f = org.json4s.DefaultFormats
            common.merge(JObject("ts" -> ts)).extract[StartupLog]
        })
    }
    
    def distinct_2(startupLogStream: DStream[StartupLog]): DStream[StartupLog] = {
        startupLogStream.transform(rdd => {
            rdd.mapPartitions(it => {
                val client: Jedis = MyRedisUtil.getClient
                val result = it.filter(startupLog => {
                    1 == client.sadd(s"dau_mids:${startupLog.logDate}", startupLog.mid)
                })
                client.close()
                result
            })
        })
        
    }
}
