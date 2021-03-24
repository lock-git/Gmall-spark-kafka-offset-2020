package com.atguigu.realtime.app

import java.lang
import java.time.LocalDate

import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.JValue
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/12/4 9:23
 */
object DauApp {
    
    def parseToStartupLog(sourceStream: InputDStream[ConsumerRecord[String, String]]) = {
        sourceStream.map(record => {
            // Gson jackson
            val j: JValue = JsonMethods.parse(record.value())
            val common: JValue = j \ "common"
            val ts = j \ "ts"
            implicit val f = org.json4s.DefaultFormats
            common.merge(JObject("ts" -> ts)).extract[StartupLog]
        })
    }
    
    // 去重, 只保留每个设备的第一次启动记录
    // 每个元素都要去连接一次redis, 导致redis的连接数过多, redis的压力太多
    def distinct(startupLogStream: DStream[StartupLog]) = {
        startupLogStream.filter(startupLog => {
            println("过滤前:" + startupLog)
            // 把设备id存入到redis的set集合, 如果返回值是1表示第一次存, 这个日志记录就保留下来. 否则, 过滤掉
            val client: Jedis = MyRedisUtil.getClient
            val r: lang.Long = client.sadd(s"dau_mids:${startupLog.logDate}", startupLog.mid)
            client.close()
            r == 1
        })
    }
    
    def distinct_2(startupLogStream: DStream[StartupLog]): DStream[StartupLog] = {
        // 每个分区建立一个到redis的连接, 这个分区内所有的数据都使用这个连接来写数据
        /*startupLogStream.mapPartitions(it => {
            val client: Jedis = MyRedisUtil.getClient
            val result = it.filter(startupLog => {
                1 == client.sadd(s"dau_mids:${startupLog.logDate}", startupLog.mid)
            })
            client.close()
            result
        })*/
        // 1. driver 代码只会执行一次
        //        val a = User("a")
        //        println(s"driver 代码只会执行一次: ${System.identityHashCode(a)}")
        startupLogStream.transform(rdd => {
            // 2. driver 每个批次执行一次
            //            println(s"driver 每个批次执行一次: ${System.identityHashCode(a)}")
            rdd.mapPartitions(it => {
                //                println(s"executor 每个分区: ${System.identityHashCode(a)}")
                // 3. executor
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
        // 1. 向创建一个StreamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 2. 从kafka消费, 得到一个流
        val sourceStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, "DauApp", "gmall_start_topic")
        // 3. 对流做各种转换
        // 3.1 解析
        val startupLogStream: DStream[StartupLog] = parseToStartupLog(sourceStream)
        // 3.2 过滤出来第一次启动记录
        val result: DStream[StartupLog] = distinct_2(startupLogStream)
        
        /*// 3.3 数据写入到es
        result.foreachRDD(rdd => {
            println("-------------------------")
            println(s"Time: ${System.currentTimeMillis()}")
            println("-------------------------")
            // 每个pi把rdd数据写入到es中
            rdd.foreachPartition((it: Iterator[StartupLog]) => {
                // 1. 创建到es的连接
                // 2. 把数据写入到es. 一个分区一写, 使用批次插入
                // 3. 关闭es
                val today: String = LocalDate.now().toString
                MyESUtil.insertBulk(s"gmall_dau_info_$today", it.map(log => (log.mid, log)))
            })
        })*/
        val today: String = LocalDate.now().toString
        result.saveToES(s"gmall_dau_info_$today")
        
        // 4. 启动实时
        ssc.start()
        ssc.awaitTermination()
    }
    
    
    
    implicit class RichStream(result: DStream[StartupLog]){
        def saveToES(index: String) = {
            result.foreachRDD(rdd => {
                println("-------------------------")
                println(s"Time: ${System.currentTimeMillis()}")
                println("-------------------------")
                // 每个pi把rdd数据写入到es中
                rdd.foreachPartition((it: Iterator[StartupLog]) => {
                    // 1. 创建到es的连接
                    // 2. 把数据写入到es. 一个分区一写, 使用批次插入
                    // 3. 关闭es
                    val today: String = LocalDate.now().toString
                    MyESUtil.insertBulk(s"gmall_dau_info_$today", it.map(log => (log.mid, log)))
                })
            })
        }
    }
    
}


/*
传递给 transform 和foreachRDD这个两个算子的匿名函数式在driver端周期的执行

其他的所有算子的传递过去的函数都是在executor执行
 */