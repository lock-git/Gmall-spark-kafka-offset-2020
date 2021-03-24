package com.atguigu.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 *
 * Date 2020/12/7 15:11
 */
object OffsetsManager {
    val mysqlUrl = "jdbc:mysql://hadoop162:3306/gmall_result?user=root&password=aaaaaa&useSSL=false"
    val queryOffsetSql =
        """
          |select
          | *
          |from ads_offset
          |where group_id=? and topic=?
          |""".stripMargin
    
    def readOffsetsFromMysql(groupId: String, topic: String): Map[TopicPartition, Long] = {
        /*
         有一个专门的表存储 offsets
         */
        JDBCUtil
            .query(mysqlUrl, queryOffsetSql, List(groupId, topic))
            .map(map => {
                new TopicPartition(topic, map("partition_id").toString.toInt) -> map("partition_offset").toString.toLong
            })
            .toMap
    }
    
    def saveOffsets(groupId: String, topic: String, offsetRanges: ListBuffer[OffsetRange]): Unit = {
        if (offsetRanges.isEmpty) {
            return
        }
        val client: Jedis = MyRedisUtil.getClient
        val key = s"offset_${groupId}_${topic}"
        val map = offsetRanges
            .map(offsetRange => {
                val partition: Int = offsetRange.partition
                val offset = offsetRange.untilOffset
                (partition.toString, offset.toString)
            })
            .toMap
            .asJava
        client.hmset(key, map) // 参数的个数不对. 原因map集合是个空集合
        println(s"保存偏移量: group=${groupId}, topic=${topic}: ${map.asScala}")
        client.close()
    }
    
    
    /**
     * 从redis读取指定消费者和指定topic的偏移量
     *
     * @param groupId
     * @param topic
     */
    def readOffsets(groupId: String, topic: String): Map[TopicPartition, Long] = {
        val key = s"offset_${groupId}_${topic}"
        val client: Jedis = MyRedisUtil.getClient
        val result = client
            .hgetAll(key)
            .asScala
            .map {
                case (partition, offset) => {
                    new TopicPartition(topic, partition.toInt) -> offset.toLong
                }
            }
            .toMap
        client.close()
        result
    }
    
    
    // TODO
    /*
    一次消费多个topic的数据, 便宜量在Redis的保存和读取问题?
    保存:
        key                             value(hash)
        "topics_${groupId}"             field               value
                                        ${topic}            "{"0": "100", "1": "200"}"    使用
                                        
                                        ---
                                        
                                       ${topic}_${partition}    100
    
     */
    def readOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
        val client: Jedis = MyRedisUtil.getClient
        val key = s"topics_${groupId}"
        val result = client
            .hgetAll(key)
            .asScala
            .flatMap {
                case (topic, json) =>
                    implicit val f = org.json4s.DefaultFormats
                    JsonMethods
                        .parse(json)
                        .extract[Map[String, String]] // {"0": "100", "1": "200"}
                        .map {
                            case (partition, offset) =>
                                (new TopicPartition(topic, partition.toInt), offset.toLong)
                        }
                
            }
            .toMap
        client.close()
        result
    }
    
    /**
     * 同时保存多个topic的消费的offset记录
     *
     * @param groupId
     * @param offsetRanges
     */
    def saveOffsets(groupId: String, offsetRanges: ListBuffer[OffsetRange]) {
        val client: Jedis = MyRedisUtil.getClient
        val key = s"topics_${groupId}"
        
        val result = offsetRanges
            .groupBy(_.topic)
            .map {
                //user_info
                case (topic, it: ListBuffer[OffsetRange]) =>
                    implicit val f = org.json4s.DefaultFormats
                    val partitionAndOffset = it
                        .map(offsetRange => (offsetRange.partition.toString, offsetRange.untilOffset.toString))
                        .toMap
                    val jsonStr = Serialization.write(partitionAndOffset)
                    (topic, jsonStr)
            }
            .asJava
        
        println("同时保存多个Topic的offsets:" + result)
        client.hmset(key, result)
        client.close()
    }
    
}
