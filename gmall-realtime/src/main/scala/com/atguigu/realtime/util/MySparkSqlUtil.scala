package com.atguigu.realtime.util

import org.apache.spark.sql.{Encoder, SparkSession}

/**
 * Author atguigu
 * Date 2020/12/11 10:37
 */
object MySparkSqlUtil {
    
    def getRDD[T: Encoder](spark: SparkSession, url: String,querySql: String) = {
        spark
            .read
            .format("jdbc")
            .option("url", url)
            .option("query", querySql)
            .load()
            .as[T]
            .rdd
    }
    
}
