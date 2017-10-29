
// uid, sid, ts  
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._

implicit class StringConversion(val s: String) {
    private def toTypeOrElse[T](convert: String=>T, defaultVal: T) = try {
        if (s matches "[\\+\\-0-9.e]+") convert(s) else defaultVal
    } catch {
        case _: NumberFormatException => defaultVal
    }
    def toShortOrElse(defaultVal: Short = 0) = toTypeOrElse[Short](_.toShort, defaultVal)
    def toByteOrElse(defaultVal: Byte = 0) = toTypeOrElse[Byte](_.toByte, defaultVal)
    def toIntOrElse(defaultVal: Int = 0) = toTypeOrElse[Int](_.toInt, defaultVal)
    def toDoubleOrElse(defaultVal: Double = 0D) = toTypeOrElse[Double](_.toDouble, defaultVal)
    def toLongOrElse(defaultVal: Long = 0L) = toTypeOrElse[Long](_.toLong, defaultVal)
    def toFloatOrElse(defaultVal: Float = 0F) = toTypeOrElse[Float](_.toFloat, defaultVal)
}
case class Shop(sid: Int, cname: String, location_id: Int, per_pay: Int
    , score: Int, comment_cnt: Int, shop_level: Int, cate_1_name: String
    , cate_2_name: String, cate_3_name: String)
val shopdata = sc.textFile("../data/shop_info2.txt").map(record => (record ++ " ").split(",")).map{
    case Array(a,b,c,d,e,f,g,h,i,j) => Shop(a.toIntOrElse(0), b, c.toIntOrElse(0), d.toIntOrElse(0), e.toIntOrElse(0), f.toIntOrElse(0), g.toIntOrElse(0), h, i, j)
}
val paydata = sc.textFile("../data/part.txt").map(record => record.split(",").map(feature => feature.toInt))
val vertices = paydata.flatMap{case Array(a, b, c) => Array((a.toLong, 0), (b.toLong, 1))}
val edges = paydata.map(record => Edge(record(0).toLong, record(1).toLong, record(2)))
val Gr = Graph(vertices, edges)
Gr.cache()
