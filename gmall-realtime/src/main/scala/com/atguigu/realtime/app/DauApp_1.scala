package com.atguigu.realtime.app

import java.time.LocalDate

import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetsManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.JValue
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/4 9:23
 */
object DauApp_1 {
    
    def parseToStartupLog(sourceStream: DStream[ConsumerRecord[String, String]]) = {
        sourceStream.map(record => {
            println(record.value())
            // Gson jackson
            val j: JValue = JsonMethods.parse(record.value())
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
    
    def main(args: Array[String]): Unit = {
        val groupId = "DauApp_1"
        val topic = "gmall_start_topic"
        
        val conf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        // 从redis读取这次应该从哪里消费
        val offsets = OffsetsManager.readOffsets(groupId, topic)
        println(s"读取偏移量: group=${groupId}, topic=${topic}:  ${offsets}")
        // 用来保存每批次的OffsetRange
        val offsetRanges = ListBuffer.empty[OffsetRange]
        
        val sourceStream = MyKafkaUtil
            .getKafkaStream(ssc, groupId, topic, offsets)
            .transform(rdd => { // 最后的时候需要保存偏移量, 只有这个地方才可以读到偏移量
                val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                offsetRanges.clear() // 考虑到每个批次都会向里写入数据, 原来的数据就是冗余
                offsetRanges ++= newOffsetRanges
                println(newOffsetRanges.map(_.fromOffset).toList)
                rdd
            })
        
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
            // 把 offsets 保存redis
            OffsetsManager.saveOffsets(groupId, topic, offsetRanges)
        })
        
        // 在这里是错误的, 会导致向redis存数据的时候出现异常
        // OffsetsManager.saveOffsets(groupId, topic, offsetRanges)
        ssc.start()
        ssc.awaitTermination()
    }
    
    
}


/*
传递给 transform 和foreachRDD这个两个算子的匿名函数式在driver端周期的执行

其他的所有算子的传递过去的函数都是在executor执行
 */