package com.atguigu.realtime

import com.atguigu.realtime.util.{MyKafkaUtil, OffsetsManager}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/*
一个流, 同时消费多个topic的数据,每个topic分别进入一个流中
 */
abstract class BaseAppV3 {
    val groupId: String
    val topics: Set[String]
    val master: String
    val appName: String
    val batchTime: Int
    
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster(master).setAppName(appName)
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(batchTime))
        
        
        // 2个topic 每个topic2个分区   => 4个k-v
        val fromOffsets: Map[TopicPartition, Long] = OffsetsManager.readOffsets(groupId, topics)
        println(s"读取偏移量: group=${groupId}, topic=${topics}:  ${fromOffsets}")
        
        // 提前创建好ListBuffer 有一个topic就有一个ListBuffer
        val topicAndOffsetRangMap: Map[String, ListBuffer[OffsetRange]] = topics
            .map(topic => (topic, ListBuffer.empty[OffsetRange])).toMap
        
        val streams: Map[String, DStream[String]] = topics
            .map(topic => {
                val stream: DStream[String] = MyKafkaUtil
                    .getKafkaStream(ssc, groupId, topic, fromOffsets.filter(_._1.topic() == topic))
                    .transform(rdd => {
                        topicAndOffsetRangMap(topic).clear() // 只清空自己的ListBuffer
                        topicAndOffsetRangMap(topic) ++= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                        rdd
                    })
                    .map(_.value())
                (topic, stream)
            })
            .toMap
        
        
        // 偏移量的写入到Redis, 子类自己负责
        run(ssc, streams, topicAndOffsetRangMap)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
    
    def run(ssc: StreamingContext,
            streams: Map[String, DStream[String]],
            topicAndOffsetRangMap: Map[String, ListBuffer[OffsetRange]])
    
    def saveOffsets(topicAndOffsetRangMap: Map[String, ListBuffer[OffsetRange]]) = {
        val result = topicAndOffsetRangMap.values.reduce(_ ++ _) // 把多个ListBuffer的值合并到一个ListBuffer中
        OffsetsManager.saveOffsets(groupId, result)
    }
    
}
