import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object FinalGraphX {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
//      .master("local[*]")
      .appName("Graphx").getOrCreate()
    val sc = spark.sparkContext

    val records: RDD[String] = sc.textFile("hdfs://s198hadoop:9000/user/hadoop/xxx/sub-trust-network.txt")
    val followers = records.map { case x => val fields = x.split("\t"); Edge(fields(0).toLong, fields(1).toLong, 1L) }
    val graph = Graph.fromEdges(followers, 1l)
    val indegrees_graph = graph.inDegrees.map(a => (a._2, a._1)).sortByKey(false, 1)
    val indegrees_graph_top = indegrees_graph.top(50).map(a => (a._2, a._1))
    //    直接输出
    //    indegrees_graph_top.foreach(s=>println(s))
        sc.parallelize(indegrees_graph_top).repartition(1).saveAsTextFile("hdfs://s198hadoop:9000/user/hadoop/xxx/resultone")
val numUsers = graph.numVertices
    val numXinren = math.round(numUsers*0.03).toInt
    val Xinren_User = sc.parallelize(indegrees_graph.top(numXinren).map(a=>(a._2,a._1)))
    val graph_join = graph.outerJoinVertices(Xinren_User){
      case(id,prop,Some(energy))=>("trust")
        case(id,prop,None)=>(1)
    }
    val olderFollowers = graph_join.aggregateMessages[Int](
      triplet=>{
        if(triplet.srcAttr.equals("trust")){
          triplet.sendToSrc(1)}
      },(a,b)=>(a+b), TripletFields.All)
    val houyue_User = olderFollowers.map(a=>(a._2,a._1)).sortByKey(false)
    val sum = math.round(houyue_User.count*0.05)
    val shangbang_User = houyue_User.top(sum.toInt).map(a=>(a._2))
sc.parallelize(shangbang_User).repartition(1).saveAsTextFile("hdfs://s198hadoop:9000/user/hadoop/xxx/resulttwo")
第三题代码：
//构造属性图，每个顶点的数学初始化为map（）
    val new_graph = graph.mapVertices((id,prop)=>Map())
    //使用aggregateMessages把verticeID和第几度邻居的度数传到出度点，出度点把信息合成大Map
    val one_neighbor = graph.aggregateMessages[Map[Long,Integer]](
      triplet=>{
        triplet.sendToDst(Map(triplet.srcId->1))
      },_++_)
    //vertice与原图进行join，更新点属性，将Map数据加入到图中
    val join_one = graph.outerJoinVertices(one_neighbor){
      (vid,data,opt)=>opt.getOrElse(Map[Long,Integer]())}.cache()
    //重复前两步，输出更新了2轮后有更新的vertice，去掉已经被顶点信任过的用户
    val two_neighbor = join_one.aggregateMessages[Map[Long,Integer]](
      ctx =>{
        val iterator = ctx.srcAttr.iterator
        while (iterator.hasNext){
          val (k,v) = iterator.next
          if (v == 1){
            val newV = 2
            ctx.sendToDst(Map(k->newV))
          }else{
            // do output and remove this entry
          }
        }
      },(newAttr,oldAttr)=>oldAttr ++ newAttr
    )
    val join_two = join_one.outerJoinVertices(two_neighbor){
      (vid,data,opt)=>opt.getOrElse(Map[Long, Integer]())++data}
    val vertice = join_two.vertices
    val result = vertice.flatMap{x=>x._2.keys.map{k=>if(k!=x._1 && x._2(k)!=1)(k,x._1)else None}}.filter(x=>x!=None)
    //将同一个用户的所有推荐用户ID合并成一个list，选取10个作为推荐结果
    val result2 = result.map{x=> val line = x.toString.replace("(","").replace(")","").split(",");(line(0).toLong,line(1).toLong)}
    val result3 = result2.combineByKey(List(_),(x:List[Long],y:Long)=>y :: x,(x : List[Long],y:List[Long])=>x ::: y)
    val result4 = result3.map{x=>(x._1,x._2.take(10))}
    result4.repartition(1).saveAsTextFile("hdfs://s198hadoop:9000/user/hadoop/xxx/resultthree")
  }
}

