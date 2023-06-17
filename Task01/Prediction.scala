import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.apache.spark.Partitioner

class MyPartitioner(numParts:Int) extends Partitioner{
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.toString.substring(0,4).toInt-2016//2016年份存到第一个分区中，下面几个年份依次存放到1-3分区
  }
}

object Prediction{
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("forecast")
    conf.setAppName("yuce")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("hdfs://s198hadoop:9000/user/hadoop/xxx/Increase/part-00000")
    val rdd1 = rdd.filter(_.length>2)
    val rdd2 = rdd1.map(line=>(line.split(",")(0),line.split(",")(1)))
    val rdd3 = rdd2.map{
      x=>{
        (x._1.replace("(", "").toString,x._2.replace(")","").toDouble)
      }
    }
    rdd3.cache()//持久化
    val queue = new mutable.Queue[Double]
    rdd3.map(
      x=>{
        queue.enqueue(x._2.toDouble)//插入字符串
        var sum:Double = 0//总数初始化为0
        var count:Int=0//平移次数初始化为0
        if(queue.length==7){ //平移的设定为7
          for(i<-queue){
            count+=1
            if(count!=7){
              sum += i
            }
          }
          val v = sum/7//计算预测涨跌幅
          val next = queue.dequeue//弹出队列第一位
          (x._1.toString,(x._2.toDouble,v.toDouble))
        }
        else{
          ("nodate".toString,(0,0))
        }
      }
    ).filter(l=>l._1.equals("nodate")==false)
      //.foreach(println)
      .partitionBy(new MyPartitioner(4)) //分成4个分区
      .saveAsTextFile("hdfs://s198hadoop:9000/user/hadoop/xxx/Prediction")
    sc.stop()
  }
}
