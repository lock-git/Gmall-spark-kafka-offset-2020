package com.atguigu.realtime.dwd

import java.time.LocalDate

import com.atguigu.realtime.BaseAppV1
import com.atguigu.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.atguigu.realtime.util.{MyESUtil, MyKafkaUtil, MySparkSqlUtil}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/11 9:37
 */
object DwdOrderInfoApp extends BaseAppV1 {
    override val groupId: String = "DwdOrderInfoApp"
    override val topic: String = "ods_order_info"
    override val master: String = "local[2]"
    override val appName: String = "DwdOrderInfoApp"
    override val batchTime: Int = 3
    
    val url = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181"
    
    /**
     * z做首单的处理
     */
    def addFirstOrderTag(spark: SparkSession,
                         orderInfoStreamWithoutDim: DStream[OrderInfo]) = {
        import spark.implicits._
        // 判断这个订单是否为这个用户的首单
        // 查询HBase
        // create table user_status(user_id varchar primary key , is_first_order boolean) SALT_BUCKETS = 5;
        orderInfoStreamWithoutDim.transform((rdd: RDD[OrderInfo]) => {
            rdd.cache()
            // 这个批次有哪些用户, 就查询哪些用户的状态  // '1', '2'    1,2
            val userIds = rdd.map(_.user_id).collect().mkString("'", "','", "'")
            val querySql = s"select * from user_status where user_id in(${userIds})"
            val oldUserIds = MySparkSqlUtil
                .getRDD[UserStatus](spark, url, querySql)
                .map(_.user_id)
                .collect()
            // 不做过滤, 首单非首单数据都在, 仅仅做了一个标记
            rdd
                .map(orderInfo => {
                    val currentUserId: Long = orderInfo.user_id
                    if (!oldUserIds.contains(currentUserId.toString)) {
                        orderInfo.is_first_order = true
                    }
                    orderInfo
                }) //如果一个用户在他下单的第一个3秒内多次下单, 都会被标记为首单. 分组, 判断是不是true, 并且个数超过1
                .groupBy(_.user_id)
                .flatMap {
                    case (user_id, it: Iterable[OrderInfo]) =>
                        val orderInfoList: List[OrderInfo] = it.toList
                        // 长度大于1并且第一个是首单,则可以判断, 里面都是首单
                        if (orderInfoList.size > 1 && orderInfoList.head.is_first_order) {
                            // 需求做处理
                            val sortedList: List[OrderInfo] = orderInfoList.sortBy(_.create_time)
                            val tmp = sortedList.tail.map(orderInfo => {
                                orderInfo.is_first_order = false
                                orderInfo
                            })
                            sortedList.head :: tmp
                        } else {
                            orderInfoList
                        }
                }
        })
    }
    
    // 补充需要的维度信息
    def addDimInfo(spark: SparkSession,
                   orderInfoStreamWithUserStatus: DStream[OrderInfo]) = {
        import spark.implicits._
        // 其实就是把省份和用户的信息查出来,和流中的数据做join
        orderInfoStreamWithUserStatus.transform(rdd => {
            rdd.cache()
            // 1. 查询省份数据
            val provinceIds = rdd.map(_.province_id).collect().mkString("'", "','", "'")
            val queryProvinceSql = s"select * from gmall_province_info where id in(${provinceIds})"
            // 2. 查询用户数据
            val userIds = rdd.map(_.user_id).collect().mkString("'", "','", "'")
            val queryUserSql = s"select * from gmall_user_info where id in(${userIds})"
            
            val provinceRDD = MySparkSqlUtil
                .getRDD[ProvinceInfo](spark, url, queryProvinceSql)
                .map(info => (info.id, info))
            val userRDD = MySparkSqlUtil
                .getRDD[UserInfo](spark, url, queryUserSql)
                .map(info => (info.id, info))
            
            // select ... from a join b on a...=b...
            rdd
                .map(orderInfo => (orderInfo.province_id.toString, orderInfo))
                .join(provinceRDD) // (key, (..., ...))
                .map {
                    case (provinceId, (orderInfo, provinceInfo)) =>
                        orderInfo.province_name = provinceInfo.name
                        orderInfo.province_area_code = provinceInfo.area_code
                        orderInfo.province_iso_code = provinceInfo.iso_code
                        (orderInfo.user_id.toString, orderInfo)
                }
                .join(userRDD)
                .map {
                    case (userId, (orderInfo, userInfo)) =>
                        orderInfo.user_age_group = userInfo.age_group
                        orderInfo.user_gender = userInfo.gender_name
                        orderInfo
                }
        })
    }
    
    override def run(ssc: StreamingContext,
                     sourceStream: DStream[String],
                     offsetRanges: ListBuffer[OffsetRange]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .config(ssc.sparkContext.getConf)
            .getOrCreate()
        
        // 1. 数据封装成样例类
        val orderInfoStreamWithoutDim: DStream[OrderInfo] = sourceStream
            .map(json => {
                implicit val f = org.json4s.DefaultFormats + myLongFormat + myDoubleFormat
                JsonMethods.parse(json).extract[OrderInfo]
            })
        // 2. 每个记录做标记: 是否首单
        val orderInfoStreamWithUserStatus: DStream[OrderInfo] = addFirstOrderTag(spark, orderInfoStreamWithoutDim)
        // 3. 读取维度表, 补充相关维度
        val resultStream = addDimInfo(spark, orderInfoStreamWithUserStatus)
        // 4. 保存
        resultStream.foreachRDD((rdd: RDD[OrderInfo]) => {
            import org.apache.phoenix.spark._
            rdd.cache()
            // 4.1. dwd层订单数据的保存: es
           /* rdd.foreachPartition((it: Iterator[OrderInfo]) => {
                
                MyESUtil.insertBulk(s"gmall_order_info_${LocalDate.now().toString}", it.map(info => (info.id.toString, info)))
            })*/
            
            // 4.1 dwd层订单数据写入到kafka中(dwd)
            rdd.foreachPartition(it => {
                val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer
                it.foreach(orderInfo => {
                    implicit val f = org.json4s.DefaultFormats
                    producer.send(new ProducerRecord[String, String]("dwd_order_info", Serialization.write(orderInfo)))
                })
                producer.close()
            })
            
            // 4.2. 保存是否首单
            rdd
                .map(orderInfo => {
                    println(orderInfo)
                    UserStatus(orderInfo.user_id.toString, orderInfo.is_first_order)
                })
                .saveToPhoenix(
                    "USER_STATUS",
                    Seq("USER_ID", "IS_FIRST_ORDER"),
                    zkUrl = Option("hadoop162,hadoop163,hadoop164:2181"))
            // 4.3. 保存偏移量
            saveOffsets(offsetRanges)
        })
    }
}
