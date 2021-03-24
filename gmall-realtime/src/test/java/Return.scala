/**
 * Author atguigu
 * Date 2020/12/5 8:55
 */
object Return {
    def main(args: Array[String]): Unit = {
        val list1 = List(30, 50, 70, 60, 10, 20)
        
        try{
            list1.foreach(x => {
                if(x < 60) return
                println(x)
            })
        }catch {
            case e => println(e)
        }
        
        
        println("xxxxxxxx")
    }
    
    def foo(x:Int):Unit = {
        if(x < 60) return
        println(x)
    }
}
