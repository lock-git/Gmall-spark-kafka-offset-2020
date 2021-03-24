package com.atguigu.realtime.dwd

import com.atguigu.realtime.BaseAppV1
import com.atguigu.realtime.bean.{OrderDetail, SkuInfo}
import com.atguigu.realtime.util.{MyKafkaUtil, MySparkSqlUtil}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/12 10:11
 */
object DwdOrderDetailApp extends BaseAppV1 {
    override val groupId: String = "DwdOrderDetailApp"
    override val topic: String = "ods_order_detail"
    override val master: String = "local[2]"
    override val appName: String = "DwdOrderDetailApp"
    override val batchTime: Int = 5
    
    override def run(ssc: StreamingContext,
                     sourceStream: DStream[String],
                     offsetRanges: ListBuffer[OffsetRange]) = {
        val phoenixUrl = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181"
        val spark: SparkSession = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        sourceStream
            .map(json => {
                implicit val f = org.json4s.DefaultFormats + myLongFormat + myDoubleFormat
                JsonMethods.parse(json).extract[OrderDetail]
            })
            .transform(rdd => {
                rdd.cache()
                val skuIds = rdd.map(_.sku_id).collect().mkString("','")
                // Join sku_info的信息
                val querySql = s"select * from gmall_sku_info where id in('${skuIds}') "
                val skuInfoRDD = MySparkSqlUtil
                    .getRDD[SkuInfo](spark, phoenixUrl, querySql)
                    .map(sku => (sku.id, sku))
                
                rdd
                    .map(detail => (detail.sku_id.toString, detail))
                    .join(skuInfoRDD)
                    .map {
                        case (_, (detail, sku)) =>
                            detail.mergeSkuInfo(sku)
                    }
            })
            .foreachRDD(rdd => {
                // 数据写入到DWD层(kafka)
                rdd.foreachPartition(it => {
                    val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer
                    it.foreach(detail => {
                        implicit val f = org.json4s.DefaultFormats
                        producer.send(new ProducerRecord[String, String]("dwd_order_detail", Serialization.write(detail)))
                    })
                    producer.close()
                    
                })
                
                saveOffsets(offsetRanges)
            })
        
        
    }
}
