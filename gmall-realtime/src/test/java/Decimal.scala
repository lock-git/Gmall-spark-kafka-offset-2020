import java.text.DecimalFormat

/**
 * Author atguigu
 * Date 2020/12/14 14:30
 */
object DecimalDemo {
    def main(args: Array[String]): Unit = {
        /*println(1- 0.9 == 0.1)
        println(1 - 0.1 == 0.9)
        println(1 - 0.9)*/
    
        println(BigDecimal(1) - BigDecimal(0.9) == BigDecimal(0.1))
        println(BigDecimal(1) / BigDecimal(3))
    
        println(Math.round(14.889))
        val f = new DecimalFormat("0000.00")
        println(f.format(14.889))
        println(f.format(4.889))
        
    }
}
