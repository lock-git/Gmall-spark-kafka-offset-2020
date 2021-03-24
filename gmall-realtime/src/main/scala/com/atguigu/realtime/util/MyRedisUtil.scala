package com.atguigu.realtime.util

import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/12/4 11:10
 */
object MyRedisUtil {
    def getClient = new Jedis("hadoop162", 6379)
}
