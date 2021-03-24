/**
 * Author atguigu
 * Date 2020/12/11 15:13
 */
object B {
    def main(args: Array[String]): Unit = {
        /*val list1 = Iterable(ABC(20))
        
        list1.toList.foreach(_.age = 30)
    
        println(list1)*/
        
        1 + 2 // 1.+(2)
        
        // 一元运算符  +1 -2 !true
        // 以冒号结尾
        
        val list1 = List(30, 50, 70, 60, 10, 20)
        list1 :+ 100 // list1.:+(100)
        
        println(100 :: list1) // list1.::(100)
        
        val list2 = List(100, 200)
        
        println(list1 ::: list2) //
        println(list1 ++ list2) //
    }
}

case class ABC(var age: Int)