import com.alibaba.fastjson.JSON
import org.json4s
import org.json4s.JValue
import org.json4s.JsonAST.{JInt, JObject}
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.beans.BeanProperty

/**
 * Author atguigu
 * Date 2020/12/4 10:16
 */
object Json4sDemo {
    
    def parse(): Unit = {
        val s =
            """
              |{
              | "name": "李四",
              | "age": 20,
              |"girls": ["a", "b"]
              |}
              |""".stripMargin
        val jValue: JValue = JsonMethods.parse(s)
        implicit val f = org.json4s.DefaultFormats
        
        //        val nameValue: JValue = jValue \ "name" // xpath
        //        val name: String = nameValue.extract[String]
        
        //        val v: JValue = jValue \ "girls"
        //        val children: List[json4s.JValue] = v.children
        //        for (elem <- children) {
        //            println(elem.extract[String])
        //        }
    
        val user: User = jValue.extract[User]
        println(user)
        
    }
    
    def write(): Unit = {
        val user = User("zs", 10, List("a", "b", "c"))
        //println(JSON.toJSONString(user, false))
        implicit val f = org.json4s.DefaultFormats
        val s: String = Serialization.write(user)
        println(s)
    }
    
    def merge(): Unit = {
        val s =
            """
              |{
              | "name": "李四",
              |"girls": ["a", "b"]
              |}
              |""".stripMargin
        implicit val f = org.json4s.DefaultFormats
        val j: JValue = JsonMethods.parse(s)
        val jAge = JObject("age" -> JInt(20))
        val r: json4s.JValue = j.merge(jAge)
        println(r)
        println(r.extract[User])
    }
    
    def main(args: Array[String]): Unit = {
//        parse()
        
//        write()
        merge()
    }
}

case class User(name: String, age: Int, girls: List[String])

/*
json4s 专门为scala设计的解析工具.
spark已经内部已经在使用, 不用额外导依赖



 */
