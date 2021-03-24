package com.atguigu.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Author atguigu
 * Date 2020/12/16 10:49
 * 从mysql读数据, 并解析
 */
object JDBCUtil {
    // select xxx form xx where id = ? ,....
    def query(url: String,
              sql: String,
              args: List[Object]): ListBuffer[Map[String, Object]] = {
        // 0. 加载驱动: 可以省略, 通过url程序可以自动找到合适的驱动
        // 1. 先得到jdbc连接  jdbc:mysql:/.../gmall?user=..paww..
        val conn: Connection = DriverManager.getConnection(url)
        // 2. 创建PrepareStatement对象
        val ps: PreparedStatement = conn.prepareStatement(sql)
        // 3. 替换占位符
        (1 to args.length).foreach(i => {
            ps.setObject(i, args(i - 1))
        })
        // 4. 执行sql
        val resultSet: ResultSet = ps.executeQuery()
        val metaData = resultSet.getMetaData // 一共多少列
        // 5. 解析结果
        val result: ListBuffer[Map[String, Object]] = ListBuffer[Map[String, Object]]()
        while (resultSet.next()) {
            // 得到一个Map集合
            // 一行有多少列?
            val lineMap = mutable.Map[String, Object]()
            val count: Int = metaData.getColumnCount
            for (columnIndex <- 1 to count) {
                val key: String = metaData.getColumnName(columnIndex)
                val value = resultSet.getObject(columnIndex)
                lineMap.put(key, value)
            }
            result += lineMap.toMap
        }
        result
    }
    
    def main(args: Array[String]): Unit = {
        val url = "jdbc:mysql://hadoop162:3306/gmall?user=root&password=aaaaaa"
        println(query(url, "select * from spu_info", Nil))
    }
}
