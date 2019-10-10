import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor
import com.alibaba.druid.util.JdbcConstants

object DruidParseSqlTest {


  def main(args: Array[String]): Unit = {
    val sql = """
                SELECT date_format(crawl_time,'%Y%m%d') as crawl_date, item_id, tax_price, crawl_time, item_url, model, jan_code, platform_name, shop_name, shop_name_eng, shop_id, title, page_model, integral, status_id, brand, category_id, category, integral_times, row_number ( ) over ( PARTITION BY time_ord  ORDER BY platform_name, CASE  WHEN shop_name IS NULL  AND platform_name = 'Rakutenn' THEN '楽天'  WHEN shop_name IS NULL  AND platform_name = 'yahoo' THEN 'ヤフー'  WHEN shop_name IS NULL THEN '' ELSE shop_name  END, item_id, length( model ) DESC, crawl_time ASC  ) AS rank_num  FROM ( SELECT concat( c.platform_name, '|', c.shop_name, '|', a.item_id ) AS time_ord, a.item_id, a.tax_price, a.crawl_time, a.item_url, b.model, b.jan_code, c.platform_name, c.shop_name, CASE  WHEN c.shop_name IN ( 'Yahoo', '楽天' ) THEN substring_index( substring_index( a.item_url, '/', 4 ), '/',- 1 ) ELSE a.shop_name_eng  END AS shop_name_eng, a.shop_id, a.title, a.page_model, changeIntegral ( a.platform, a.integral, a.pretax_price, a.tax_price ) AS integral, b.brand AS brand, changeStatusId ( a.platform, a.sellOutString, a.status_id ) AS status_id, b.category_id, b.category_fl AS category, a.integral_times, b.collect_type, length( b.model ) AS model_len  FROM ( SELECT item_id, jan_code, mutliReplace ( tax_price ) AS tax_price, mutliReplace ( pretax_price ) AS pretax_price, from_unixtime( cast( fetch_time / 1000 AS BIGINT ), 'yyyy/MM/dd HH:mm:ss' ) AS crawl_time, item_url, shop_id, title, page_model, platform, sellOutString, status_id, integral, integral_times, shop_name_eng, changeShopName ( platform, shop_id, shop_name ) AS shop_name  FROM t_sale_price_monitor_jp_test  ) a JOIN t_custom_model b ON a.jan_code = b.jan_code JOIN t_custom_shop c ON ifnull( c.shop_name_alias, ifnull( a.shop_name, '' ) ) = ifnull( a.shop_name, '' )  AND c.platform_name_alias = a.platform JOIN t_category d ON b.category_id = d.category_id  WHERE length( a.item_id ) > 0  AND ( regexp_extract ( REPLACE ( REPLACE ( a.title, ' ', '-' ), '-', '' ), REPLACE ( REPLACE ( REPLACE ( REPLACE ( b.model, ' ', '(' ), '-', '[-|(]?' ), '(', '[-| |(]?' ), ')', ''  ), 0  ) != ''  OR regexp_extract ( REPLACE ( a.title, '−', '' ), REPLACE ( REPLACE ( REPLACE ( REPLACE ( b.model_alias, '　', '（' ), '－', '[‐|−|－|（]?' ), '（', '[‐|−|－|　|（]?' ), '）', ''  ), 0  ) != ''  )  AND a.tax_price >= d.lowest_price  AND b.brand = '东芝'  AND regexp_extract ( a.title, '専用加入料|加入料のみ注文不可|wifi', 0 ) = '' )AS t  WHERE t.rank_num = 1
              """
    val dbType = JdbcConstants.HIVE
//    val mysqlFormat = SQLUtils.format(sql, dbType)
    val sqlList = SQLUtils.parseStatements(sql, dbType)
    for (i <- 0 until sqlList.size()) {
      val stmt = sqlList.get(i)
      val version = new MySqlSchemaStatVisitor()
      stmt.accept(version)
      //获取表名称
      println("Tables : " + version.getTables.keySet())
      //获取操作方法名称,依赖于表名称
      println("Manipulation : " + version.getTables)
      //获取字段名称
      println("fields : " + version.getColumns)
    }

  }
}
