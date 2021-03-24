package com.atguigu.realtime.dws

import java.text.DecimalFormat
import java.util.Properties

import com.atguigu.realtime.BaseAppV3
import com.atguigu.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.realtime.util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.{JsonMethods, Serialization}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/12 14:25
 * 消费两个topic, 这两个topic放入两个不同流中, 进行join
 */
object DwsOrderWideApp extends BaseAppV3 {
    override val groupId: String = "DwsOrderWideApp8"
    override val topics: Set[String] = Set("dwd_order_info", "dwd_order_detail")
    override val master: String = "local[4]"
    override val appName: String = "DwsOrderWideApp"
    override val batchTime: Int = 5
    
    val preOrderInfo = "order_info:"
    val preOrderDetail = "order_detail:"
    
    def join_1(orderInfoStream: DStream[OrderInfo],
               orderDetailStream: DStream[OrderDetail]) = {
        val orderInfoStreamWindow = orderInfoStream
            .map(info => (info.id, info))
            .window(Seconds(batchTime * 4), Seconds(batchTime))
        val orderDetailStreamWindow = orderDetailStream
            .map(detail => (detail.order_id, detail))
            .window(Seconds(batchTime * 4), Seconds(batchTime))
        
        // 去重: 借助redis set集合:
        orderInfoStreamWindow
            .join(orderDetailStreamWindow)
            .map {
                case (order_id, (info, detail)) =>
                    // OrderWide.apply(info, detail)
                    new OrderWide(info, detail) // 如果调用的是辅助构造器必须使用new
            }
            .mapPartitions(it => {
                val client = MyRedisUtil.getClient
                
                val result = it.filter(wide => {
                    1 == client.sadd("order_detail_id_set", wide.order_detail_id.toString)
                })
                client.close()
                result
            })
        
        
    }
    
    // TODO
    def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo)(implicit f: org.json4s.DefaultFormats) = {
        val orderInfoKey = preOrderInfo + orderInfo.id
        /*client.set(orderInfoKey, Serialization.write(orderInfo))
        client.expire(orderInfoKey, 30 * 60)*/
        client.setex(orderInfoKey, 30 * 60, Serialization.write(orderInfo))
    }
    
    //TODO
    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail)(implicit f: org.json4s.DefaultFormats) = {
        client.hset(preOrderDetail + orderDetail.order_id, orderDetail.id.toString, Serialization.write(orderDetail))
        
    }
    
    def join_2(orderInfoStream: DStream[OrderInfo],
               orderDetailStream: DStream[OrderDetail]) = {
        val stream1 = orderInfoStream
            .map(info => (info.id, info))
        
        val stream2 = orderDetailStream
            .map(detail => (detail.order_id, detail))
        
        stream1
            .fullOuterJoin(stream2)
            .mapPartitions(it => {
                implicit val f = org.json4s.DefaultFormats
                val client: Jedis = MyRedisUtil.getClient
                val result = it.flatMap {
                    case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                        println("some some")
                        cacheOrderInfo(client, orderInfo)
                        
                        val orderWide = new OrderWide(orderInfo, orderDetail)
                        
                        val key = preOrderDetail + orderId
                        if (client.exists(key)) {
                            // 2. 找到
                            val r = client
                                .hgetAll(key)
                                .asScala
                                .map {
                                    case (orderDetailId, orderDetailJson) =>
                                        val orderDetail = JsonMethods.parse(orderDetailJson).extract[OrderDetail]
                                        new OrderWide(orderInfo, orderDetail)
                                }
                                .toList
                            // order_detail的信息只能被用一次, 所以, 一旦被join, 则必须删除
                            client.del(key)
                            orderWide :: r
                        } else {
                            // 3. 找不到
                            orderWide :: Nil
                        }
                    
                    case (orderId, (Some(orderInfo), None)) =>
                        println("some None")
                        // 0. 必须缓存order_info
                        cacheOrderInfo(client, orderInfo)
                        // 1. 去对方的缓存
                        val key = preOrderDetail + orderId
                        if (client.exists(key)) {
                            // 2. 找到
                            val r = client
                                .hgetAll(key)
                                .asScala
                                .map {
                                    case (orderDetailId, orderDetailJson) =>
                                        val orderDetail = JsonMethods.parse(orderDetailJson).extract[OrderDetail]
                                        new OrderWide(orderInfo, orderDetail)
                                }
                            // order_detail的信息只能被用一次, 所以, 一旦被join, 则必须删除
                            client.del(key)
                            r
                        } else {
                            // 3. 找不到
                            Nil
                        }
                    case (orderId, (None, Some(orderDetail))) =>
                        println("None some")
                        // 如果join上就返回元素, join不需要返回, 必须使用flatMap
                        
                        // 1. 去order_info的缓存查找对应order_info信息
                        val key = preOrderInfo + orderId
                        if (client.exists(key)) {
                            // 2. 找到了, 封装到一起成为OrderWide
                            val orderInfoJson: String = client.get(key)
                            val orderInfo = JsonMethods.parse(orderInfoJson).extract[OrderInfo]
                            new OrderWide(orderInfo, orderDetail) :: Nil
                        } else {
                            // 3. 没找到, 把order_detail存入到缓存
                            cacheOrderDetail(client, orderDetail)
                            Nil
                        }
                }
                client.close()
                result
            })
        
        
    }
    
    def join_3(orderInfoStream: DStream[OrderInfo],
               orderDetailStream: DStream[OrderDetail]) = {
        val stream1 = orderInfoStream
            .map(info => (info.id, info))
        
        val stream2 = orderDetailStream
            .map(detail => (detail.order_id, detail))
        
        stream1
            .fullOuterJoin(stream2)
            .mapPartitions(it => {
                implicit val f = org.json4s.DefaultFormats
                val client: Jedis = MyRedisUtil.getClient
                val result = it.flatMap {
                    case (orderId, (Some(orderInfo), opt)) =>
                        println("some opt")
                        cacheOrderInfo(client, orderInfo)
                        
                        val tmp = opt match {
                            case Some(orderDetail) =>
                                new OrderWide(orderInfo, orderDetail) :: Nil
                            case None =>
                                Nil
                        }
                        
                        val key = preOrderDetail + orderId
                        if (client.exists(key)) {
                            // 2. 找到
                            val r = client
                                .hgetAll(key)
                                .asScala
                                .map {
                                    case (orderDetailId, orderDetailJson) =>
                                        val orderDetail = JsonMethods.parse(orderDetailJson).extract[OrderDetail]
                                        new OrderWide(orderInfo, orderDetail)
                                }
                                .toList
                            // order_detail的信息只能被用一次, 所以, 一旦被join, 则必须删除
                            client.del(key)
                            tmp ::: r
                        } else {
                            // 3. 找不到
                            tmp ::: Nil
                        }
                    
                    case (orderId, (None, Some(orderDetail))) =>
                        println("None some")
                        // 如果join上就返回元素, join不需要返回, 必须使用flatMap
                        
                        // 1. 去order_info的缓存查找对应order_info信息
                        val key = preOrderInfo + orderId
                        if (client.exists(key)) {
                            // 2. 找到了, 封装到一起成为OrderWide
                            val orderInfoJson: String = client.get(key)
                            val orderInfo = JsonMethods.parse(orderInfoJson).extract[OrderInfo]
                            new OrderWide(orderInfo, orderDetail) :: Nil
                        } else {
                            // 3. 没找到, 把order_detail存入到缓存
                            cacheOrderDetail(client, orderDetail)
                            Nil
                        }
                }
                client.close()
                result
            })
        
        
    }
    
    def moneyShare(orderWideStream: DStream[OrderWide]) = {
        val originKey = "origin_total"
        val payKey = "pay_total"
        orderWideStream.mapPartitions(it => {
            val decimalFormat = new DecimalFormat("0000.00")
            
            val client: Jedis = MyRedisUtil.getClient
            val result = it.map(orderWide => {
                // 存储的hash的field的值, 表示每个顶点的前面的金额情况
                val field = orderWide.order_id.toString
                // 1. 针对某个订单: 获取前面的详情的总的原始金额
                val originTotal =
                    if (client.hexists(originKey, field)) client.hget(originKey, field).toDouble
                    else 0D
                
                // 2. 获取前面的详情的总的分摊金额
                val payTotal =
                    if (client.hexists(payKey, field)) client.hget(payKey, field).toDouble
                    else 0D
                
                // 3. 判断当前详情是否为最后一个详情
                val currentOrigin = orderWide.sku_num * orderWide.sku_price
                if (BigDecimal(currentOrigin) == BigDecimal(orderWide.original_total_amount) - BigDecimal(originTotal)) {
                    // 3.1 是最后一个详情: 计算分摊.   总的支付-payTotal
                    val currentShare = BigDecimal(orderWide.final_total_amount) - BigDecimal(payTotal)
                    orderWide.final_detail_amount = currentShare.toDouble
                } else {
                    // 3.2 不是最后一个详情: 计算自己的分摊: 单价*个数*支付总金额/原始总金额  把当前和元素的和写入到redis
                    val currentShare = orderWide.sku_price * orderWide.sku_num * orderWide.final_total_amount / orderWide.original_total_amount
                    orderWide.final_detail_amount = decimalFormat.format(currentShare).toDouble
                    // currentShare + payTotal
                    // 前面的支付的总额
                    client.hset(payKey, field, decimalFormat.format(currentShare + payTotal))
                    // 前面的原价总额
                    client.hset(originKey, field, decimalFormat.format(currentOrigin + originTotal))
                }
                orderWide
            })
            client.close()
            result
        })
    }
    
    override def run(ssc: StreamingContext,
                     streams: Map[String, DStream[String]],
                     topicAndOffsetRangMap: Map[String, ListBuffer[OffsetRange]]): Unit = {
        
        val orderInfoStream = streams("dwd_order_info")
            .map(json => {
                implicit val f = org.json4s.DefaultFormats
                JsonMethods.parse(json).extract[OrderInfo]
            })
        val orderDetailStream = streams("dwd_order_detail")
            .map(json => {
                implicit val f = org.json4s.DefaultFormats
                JsonMethods.parse(json).extract[OrderDetail]
            })
        
        val orderWideStream = join_3(orderInfoStream, orderDetailStream)
        
        val resultStream = moneyShare(orderWideStream)
        
        val spark: SparkSession = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
        val clickHouseUrl = "jdbc:clickhouse://hadoop162:8123/gmall";
        import spark.implicits._
        // clickhouse
        resultStream.foreachRDD(rdd => {
            // 把写入到cickhouse中
            /*rdd
                .toDS
                .write
                .option("batchsize", "100")
                .option("isolationLevel", "NONE") // 设置没有事务
                .option("numPartitions", "2") // 设置并发
                //.option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
                .mode("append")
                .jdbc(clickHouseUrl, "order_wide", new Properties());*/
            
            // 把宽表数据写入到Kafka, 给ADS层准备数据
            rdd.foreachPartition(it => {
                implicit val f = org.json4s.DefaultFormats
                val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer
                it.foreach(wide => {
                    producer.send(new ProducerRecord[String, String]("dws_order_wide", Serialization.write(wide)))
                })
                
                producer.close()
            })
            
            saveOffsets(topicAndOffsetRangMap)
        })
    }
}

/*
order_info如何存入到redis:
    key                                 value(string)  json格式的字符串
    "order_info:" + order_id            {"":"", "": ""}
    

order_detail如何存入到redis:
    key                                                 value(hash)
    "order_detail:" + order_id                          field                   value
                                                        order_detail_id         json字符串
    
-----------
订单              详情 1(单价*个数)                详情 2         详情3
99                  33                              33             33
89                  89/3                            89/3           89/3
                 = 单价*个数*只父总金额/原始总金额
                 
                 
1. 一个订单对应多个详情, 计算分摊的时候, 由于四舍五入导致结果的和与实际支付不符合
2. 解决方案: 假设3个详情, 前两个就使用正常计算的结果, 最后一个用总支付 - 前面的支付和
3. 如果判断某个详情是这个订单的最后一个详情?
    原始总价-前面详情的总价 == 当前详情的总价   就是最后!
 
4. 如果知道前面的详情的支付和?  如何知道前面详情的总价?
    这些中间值存储到redis中!
    会用什么数据结构?
    string:
        key                                     value
        "order_wide_share_pay:" + order_id      100
    
    hash:
        key                                     value
        "order_wide_share_pay"                  field           value
                                                order_id        200
                                                

 
 */
















