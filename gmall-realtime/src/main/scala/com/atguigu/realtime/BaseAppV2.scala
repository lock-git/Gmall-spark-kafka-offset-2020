package com.atguigu.realtime

import com.atguigu.realtime.util.{MyKafkaUtil, OffsetsManager}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/*
一个流, 同时消费多个topic的数据
 */
abstract class BaseAppV2 {
    val groupId: String
    val topics: Set[String]
    val master: String
    val appName: String
    val batchTime: Int
    
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster(master).setAppName(appName)
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchTime))
        
        
        val fromOffsets: Map[TopicPartition, Long] = OffsetsManager.readOffsets(groupId, topics)
        println(s"读取偏移量: group=${groupId}, topic=${topics}:  ${fromOffsets}")
        
        val offsetRanges: ListBuffer[OffsetRange] = ListBuffer.empty[OffsetRange]
        val sourceStream = MyKafkaUtil
            .getKafkaStream(ssc, groupId, topics, fromOffsets)
            .transform(rdd => {
                offsetRanges.clear()
                offsetRanges ++= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            })
            .map(record => {
                (record.topic(), record.value())
            })
        
        // 偏移量的写入到Redis, 子类自己负责
        run(ssc, sourceStream, offsetRanges)
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    def run(ssc: StreamingContext, sourceStream: DStream[(String, String)], offsetRanges: ListBuffer[OffsetRange])
    
}
