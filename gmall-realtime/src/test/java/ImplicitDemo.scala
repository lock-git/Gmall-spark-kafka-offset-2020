import java.time.LocalDate

/**
 * Author atguigu
 * Date 2020/12/7 11:28
 */
object ImplicitDemo {
    def main(args: Array[String]): Unit = {
        val ago = "ago"
        val later = "later"
        70 days ago // 2.days(ago)
        70 days later // 2.days(ago)
    }
    
    implicit class RichInt(day: Int) {
        def days(when: String) = {
            
            val r = when match {
                case "ago" =>
                    LocalDate.now().minusDays(day)
                case "later" =>
                    LocalDate.now().plusDays(day)
            }
            println(r)
        }
    }
    
}

