import java.io.StringReader
import java.util

object SqlStringUtils {
  def changeSqlStr(sql: String): String = {
    val strb = new StringBuilder()
    val token = "FROM"
    val sqlArr = sql.toUpperCase.split(token)
    null
  }
}
