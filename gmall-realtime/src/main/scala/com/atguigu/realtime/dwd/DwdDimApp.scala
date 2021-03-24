package com.atguigu.realtime.dwd

import java.util.Properties

import com.atguigu.realtime.BaseAppV2
import com.atguigu.realtime.bean._
import com.atguigu.realtime.util.OffsetsManager
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

/**
 * 把ods中维度表的数据写入到hbase中, 利用Phoenix
 * 一个类, 负责把需要用到的所有维度表数据同步到Phoenix
 *
 * 1. 先从ods消费数据
 * 多个topic
 * a: 一个流中只有一个topic的数据
 * b: 一个流中有多个topic的数据
 * 2. 写入到hbase
 */
object DwdDimApp extends BaseAppV2 {
    override val groupId: String = "DwdDimApp"
    override val topics: Set[String] = Set(
        "ods_user_info",
        "ods_base_province",
        "ods_sku_info",
        "ods_spu_info",
        "ods_base_category3",
        "ods_base_trademark")
    
    override val master: String = "local[2]"
    override val appName: String = "DwdDimApp"
    override val batchTime: Int = 60
    
    def saveToHBase[T <: Product](rdd: RDD[(String, String)],
                                  topic: String, table:
                                  String, cols: Seq[String])(implicit mf: scala.reflect.Manifest[T]) = {
        
        rdd
            .filter(_._1 == topic)
            .map {
                case (_, value) =>
                    implicit val f = org.json4s.DefaultFormats
                    JsonMethods.parse(value).extract[T]
            }
            .saveToPhoenix(table, cols, zkUrl = Option("hadoop162,hadoop163,hadoop164:2181"))
    }
    
    override def run(ssc: StreamingContext,
                     sourceStream: DStream[(String, String)],
                     offsetRanges: ListBuffer[OffsetRange]): Unit = {
        
        val spark: SparkSession = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val phoenixUrl = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181"
        
        sourceStream
            .foreachRDD((rdd: RDD[(String, String)]) => {
                // 保存UserInfo
                saveToHBase[UserInfo](
                    rdd,
                    "ods_user_info",
                    "gmall_user_info",
                    Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"))
                saveToHBase[ProvinceInfo](
                    rdd,
                    "ods_base_province",
                    "gmall_province_info",
                    Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"))
                
                saveToHBase[SpuInfo](rdd,
                    "ods_spu_info",
                    "gmall_spu_info",
                    Seq("ID", "SPU_NAME"))
                
                saveToHBase[BaseCategory3](rdd,
                    "ods_base_category3",
                    "gmall_base_category3",
                    Seq("ID", "NAME", "CATEGORY2_ID"))
                
                saveToHBase[BaseTrademark](rdd,
                    "ods_base_trademark",
                    "gmall_base_trademark",
                    Seq("ID", "TM_NAME"))
                
                
                
                // 先补齐相关维度, 再写SkuInfo
                spark.read.jdbc(phoenixUrl, "gmall_base_category3", new Properties()).createOrReplaceTempView("c3")
                spark.read.jdbc(phoenixUrl, "gmall_spu_info", new Properties()).createOrReplaceTempView("spu")
                spark.read.jdbc(phoenixUrl, "gmall_base_trademark", new Properties()).createOrReplaceTempView("tm")
                
                // sku没有join之前的数据
                rdd
                    .filter(_._1 == "ods_sku_info")
                    .map {
                        case (_, json) =>
                            implicit val f = org.json4s.DefaultFormats
                            JsonMethods.parse(json).extract[SkuInfo]
                    }
                    .toDF()
                    .createOrReplaceTempView("sku")
                
                //直接使用Phoenix的写法
                spark
                    .sql(
                        """
                          |select
                          |    sku.id as id,
                          |    sku.spu_id spu_id,
                          |    sku.price price,
                          |    sku.sku_name sku_name,
                          |    sku.tm_id  tm_id,
                          |    sku.category3_id  category3_id,
                          |    sku.create_time  create_time,
                          |    c3.name  category3_name,
                          |    spu.spu_name  spu_name,
                          |    tm.tm_name  tm_name
                          |from sku
                          |join spu on sku.spu_id=spu.id
                          |join c3 on sku.category3_id=c3.id
                          |join tm on sku.tm_id=tm.id
                          |""".stripMargin)
                    .as[SkuInfo]
                    .rdd
                    .saveToPhoenix(
                        "gmall_sku_info",
                        Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"),
                        zkUrl = Option("hadoop162,hadoop163,hadoop164:2181"))
                
                OffsetsManager.saveOffsets(groupId, offsetRanges)
            })
    }
}
