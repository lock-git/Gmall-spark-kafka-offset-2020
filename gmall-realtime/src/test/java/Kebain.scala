/**
 * Author atguigu
 * Date 2020/12/17 9:56
 */
object Kebain {
    def main(args: Array[String]): Unit = {
        foo(1, 2, 3)
        
        val list1 = List(30, 50, 70, 60, 10, 20)
        foo(list1: _*)
    }
    
    def foo(a: Int*) = {
        for (elem <- a) {
            println(elem)
        }
    }
}
