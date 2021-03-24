package com.atguigu.realtime

import com.atguigu.realtime.util.{MyKafkaUtil, OffsetsManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JDouble, JInt, JString}

import scala.collection.mutable.ListBuffer

abstract class BaseAppV1 {
    val groupId: String
    val topic: String
    val master: String
    val appName: String
    val batchTime: Int
    
    
    val myLongFormat = new CustomSerializer[Long](format =>
        ( {
            case JString(s) => s.toLong
            case JInt(num) => num.toLong
        }, {
            case n: Long => JString(n.toString)
        }))
    
    val myDoubleFormat = new CustomSerializer[Double](format =>
        ( {
            case JString(s) => s.toDouble
            case JDouble(num) => num.toDouble
        }, {
            case n: Double => JString(n.toString)
        }
        ))
    
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster(master).setAppName(appName)
        val ssc = new StreamingContext(conf, Seconds(batchTime))
        
        // 从redis读取这次应该从哪里消费
        val offsets = OffsetsManager.readOffsets(groupId, topic)
        println(s"读取偏移量: group=${groupId}, topic=${topic}:  ${offsets}")
        // 用来保存每批次的OffsetRange
        val offsetRanges: ListBuffer[OffsetRange] = ListBuffer.empty[OffsetRange]
        
        val sourceStream = MyKafkaUtil
            .getKafkaStream(ssc, groupId, topic, offsets)
            .transform(rdd => { // 最后的时候需要保存偏移量, 只有这个地方才可以读到偏移量
                val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                offsetRanges.clear() // 考虑到每个批次都会向里写入数据, 原来的数据就是冗余
                offsetRanges ++= newOffsetRanges
                rdd
            })
            .map(_.value())
        
        
        // 具体的业务
        run(ssc, sourceStream, offsetRanges)
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    
    def saveOffsets(offsetRanges: ListBuffer[OffsetRange]): Unit = {
        OffsetsManager.saveOffsets(groupId, topic, offsetRanges)
    }
    
    def run(ssc: StreamingContext,
            sourceStream: DStream[String],
            offsetRanges: ListBuffer[OffsetRange]): Unit
    
    
}
