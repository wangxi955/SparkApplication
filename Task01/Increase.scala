import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}
import scala.collection.mutable

object Increase{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("zhangfu")
//    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val data=sc.textFile("hdfs://s198hadoop:9000/user/hadoop/xxx/000009.csv")

    val date_close=data.map(s=>s.split(","))
    val date_close1=date_close.map(x=>(x(0),x(4).toFloat)).filter(x=>x._2!=0)

    val queue=new mutable.Queue[Double]()
    val zhangfu=date_close1.map{
      x=>
        queue.enqueue(x._2)
        if (queue.length==2){
          val y=((queue.last-queue.head)/queue.head);
          queue.dequeue()
          (x._1,y)
        }else{
          (x._1,0)
        }
    }.saveAsTextFile("hdfs://s198hadoop:9000/user/hadoop/xxx/Increase")
//      .foreach(x=>println(x))
  }
}
