package com.atguigu.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

import scala.collection.JavaConverters._

/**
 * Author atguigu
 * Date 2020/12/7 9:19
 */
object MyESUtil {
    // 1. 创建一个到es连接的客户端
    val factory = new JestClientFactory
    val urls = List("http://hadoop162:9200", "http://hadoop163:9200").asJava
    val conf = new HttpClientConfig.Builder(urls)
        .connTimeout(10 * 1000)
        .readTimeout(10 * 1000)
        .multiThreaded(true)
        .maxTotalConnection(100)
        .build()
    factory.setHttpClientConfig(conf)
    
    /**
     * 单条插入
     *
     * @param index
     * @param source
     * @param id
     */
    def insertSingle(index: String, source: Object, id: String = null): Unit = {
        val client: JestClient = factory.getObject
        val action: Index = new Index.Builder(source)
            .index(index)
            .`type`("_doc")
            .id(id) //如果id的值是null, 会随机生成id
            .build()
        client.execute(action)
        client.close()
    }
    
    def insertBulk(index: String, sources: Iterator[Object]) = {
        val client: JestClient = factory.getObject
        val bulkBuilder = new Bulk.Builder()
            .defaultIndex(index)
            .defaultType("_doc")
        /*sources
            .map(source => {
                new Index.Builder(source).build()
            })
            .foreach(bulkBuilder.addAction)*/
        
        /*for (source <- sources) {
            source match {
                case (id: String, v) =>
                    val index: Index = new Index.Builder(v).id(id).build()
                    bulkBuilder.addAction(index)
                case v =>
                    val index: Index = new Index.Builder(v).build()
                    bulkBuilder.addAction(index)
            }
            
        }*/
        sources.map {
            case (id: String, v) =>
                new Index.Builder(v).id(id).build()
            case v =>
                new Index.Builder(v).build()
        }.foreach(bulkBuilder.addAction)
        
        client.execute(bulkBuilder.build())
        
        client.close()
    }
    
    
    def main(args: Array[String]): Unit = {
        
        val sources = Iterator(
            ("aa", User1("a", 1)),
            """{"name": "b", "age": 2}""")
        insertBulk("user1", sources)
        
    }
}

case class User1(name: String, age: Int)