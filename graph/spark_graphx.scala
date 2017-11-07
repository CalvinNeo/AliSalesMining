
// uid, sid, ts  
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag

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
case class ShopInfo(sid: Int, cname: String, location_id: Int, per_pay: Int
    , score: Int, comment_cnt: Int, shop_level: Int, cate_1_name: String
    , cate_2_name: String, cate_3_name: String)
case class UserInfo(uid: Int)
case class Shop(sid: Int)
case class User(uid: Int)
val shopdata = sc.textFile("../data/shop_info.txt").map(record => (record ++ " ").split(",")).map{
    case Array(a,b,c,d,e,f,g,h,i,j) => new ShopInfo(a.toIntOrElse(0), b, c.toIntOrElse(0), d.toIntOrElse(0), e.toIntOrElse(0), f.toIntOrElse(0), g.toIntOrElse(0), h, i, j)
}
val paydata = sc.textFile("../data/part.txt").map(record => record.split(",").map(feature => feature.toInt))
val userdata = paydata.map{case Array(a, b, c) => UserInfo(a)}.distinct
val vertices = paydata.flatMap{case Array(a, b, c) => Array((a.toLong, User(a)), (b.toLong, Shop(b)))}
val edges = paydata.map(record => Edge(record(0).toLong, record(1).toLong, record(2)))
val Gr = Graph(vertices, edges)
Gr.cache()

def sortedConnectedComponents(conn: Graph[VertexId, _]): Seq[(VertexId, Long)] = {
    val conn_counts = conn.vertices.map(_._2).countByValue
    conn_counts.toSeq.sortBy(_._2).reverse
}
def removeSingletons[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) = {
    // make verts from edges
    val p1 = g.triplets.map(edge_triplet => (edge_triplet.srcId, edge_triplet.srcAttr))
    val p2 = g.triplets.map(edge_triplet => (edge_triplet.dstId, edge_triplet.dstAttr))
    Graph(p1.union(p2).distinct, g.edges)
}

val sampled = Gr.vertices.map(v => v._1).sample(false, 0.01, 1729L)
val degrees = Gr.degrees
val parGr = removeSingletons(Gr.subgraph(edge_triplet => edge_triplet.attr < 1447308000))
parGr.cache()
println("origin vert count " ++ Gr.vertices.count().toString)
println("current vert count " ++ parGr.vertices.count().toString)
val bfsGr = parGr.mapVertices((id, _) => {
    Map(id -> 0)
})

// pregel
def mergeMa(m1: Map[VertexId, Int], m2: Map[VertexId, Int]) : Map[VertexId, Int] = {
    def minEx(k: VertexId): Int = {
        math.min(m1.getOrElse(k, Int.MaxValue), m2.getOrElse(k, Int.MaxValue))
    }
    (m1.keySet ++ m2.keySet).map{k => (k, minEx(k))}.toMap
}
def updateMa(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int]) = {
    mergeMa(state, msg)
}
def iterateMa(e: EdgeTriplet[Map[VertexId, Int], _]) = {
    def inform(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId) = {
        val from_a = a.map{case (v, d) => v -> (d + 1)}
        if(b != mergeMa(from_a, b)){
            Iterator((bid, from_a))
        }else{
            Iterator.empty
        }
    }
    inform(e.srcAttr, e.dstAttr, e.dstId) ++ inform(e.dstAttr, e.srcAttr, e.srcId)
}

val dist0Gr = Map[VertexId, Int]()
val distGr = bfsGr.pregel(dist0Gr)(updateMa, iterateMa, mergeMa)
val distRes = distGr.vertices.collect
def recommendShop(uid: Int) : List[(VertexId, Int)] = {
    val handle = distRes.filter(_._1 == uid)(0)
    handle._2.toList.sortWith(_._2 < _._2).filter(tp => tp._1 != uid && tp._1 < 9999)
}
def recommendUser(sid: Int) : List[(VertexId, Int)] = {
    val handle = distRes.filter(_._1 == sid)(0)
    handle._2.toList.sortWith(_._2 < _._2).filter(tp => tp._1 != sid && tp._1 > 9999)
}
def printRecommend(id: Int) = {
    if(id < 9999){
        val all_possible = recommendUser(id)
        all_possible.take(3).foreach(item => {
            val index = item._1
            userdata.filter(x => x.uid == index).foreach(println)
        })
    }else{
        val all_possible = recommendShop(id)
        all_possible.take(3).foreach(item => {
            val index = item._1
            shopdata.filter(x => x.sid == index).foreach(println)
        })
    }
}
