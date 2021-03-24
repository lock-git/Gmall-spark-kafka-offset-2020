import java.time.{LocalDateTime, LocalTime}

/**
 * Author atguigu
 * Date 2020/12/16 14:05
 */
object CC {
    def main(args: Array[String]): Unit = {
        val time: LocalDateTime = LocalDateTime.now()
        val statTime = s"${time.toLocalDate} ${time.toLocalTime.toString.substring(0, 8)}"
        println(statTime)
    }
}
