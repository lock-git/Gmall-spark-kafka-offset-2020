package com.atguigu.realtime.util

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConverters._

object MyKafkaUtil {
    
    import org.apache.kafka.common.serialization.StringDeserializer
    
    var kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "hadoop162:9092,hadoop163:9092,hadoop164:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    
    def getKafkaStream(ssc: StreamingContext,
                       groupId: String,
                       topic: String) = {
        kafkaParams += "group.id" -> groupId
        KafkaUtils
            .createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Subscribe[String, String](Set(topic), kafkaParams)
            )
    }
    
    /**
     * 从指定的位置开始消费Kafka的topic
     *
     * @param ssc
     * @param groupId
     * @param topic
     * @param offsets
     * @return
     */
    def getKafkaStream(ssc: StreamingContext,
                       groupId: String,
                       topic: String,
                       offsets: Map[TopicPartition, Long]) = {
        kafkaParams += "group.id" -> groupId
        kafkaParams += "enable.auto.commit" -> (false: java.lang.Boolean)
        KafkaUtils
            .createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Subscribe[String, String](Set(topic), kafkaParams, offsets)
            )
    }
    
    /**
     * 可以同时消费多个topic
     * @param ssc
     * @param groupId
     * @param topics
     * @param offsets
     * @return
     */
    def getKafkaStream(ssc: StreamingContext,
                       groupId: String,
                       topics: Set[String],
                       offsets: Map[TopicPartition, Long]) = {
        kafkaParams += "group.id" -> groupId
        kafkaParams += "enable.auto.commit" -> (false: java.lang.Boolean)
        KafkaUtils
            .createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Subscribe[String, String](topics, kafkaParams, offsets)
            )
    }
    
    val producerParams = Map[String, Object](
        "bootstrap.servers" -> "hadoop162:9092,hadoop163:9092,hadoop164:9092",
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "enable.idempotent" -> (true: java.lang.Boolean)
    ).asJava
    
    def getKafkaProducer: KafkaProducer[String, String] = {
        new KafkaProducer[String, String](producerParams)
    }
}
