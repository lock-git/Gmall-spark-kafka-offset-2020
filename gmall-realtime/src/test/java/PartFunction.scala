/**
 * Author atguigu
 * Date 2020/12/9 9:07
 */
object PartFunction {
    def main(args: Array[String]): Unit = {
        val list1 = List(30, 50, 70, 60, 10, 20)
        
        list1.foreach(x => println(x + 1))
        list1.foreach( println(_))
        list1.foreach( println)
        
        
    
    
        val f: Double => Double = math.pow(_, 2)
        println(f(20))
        println(f(30))
    
        val f1: Any => Unit = println(_ )
        f1(20)
       
    }
}
