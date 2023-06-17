import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object FileStreamingTest{

  def main(args: Array[String]): Unit = {

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val sparkConf = new SparkConf().setAppName("JDprice").setMaster("local[2]")//设置为本地运行模式，2个线程，一个监听，另一个处理数据
    val sparkConf = new SparkConf().setAppName("JDprice")
    val ssc = new StreamingContext(sparkConf, Seconds(5))//时间间隔为5s

    ssc.sparkContext.setLogLevel("ERROR")

    val lines = ssc.textFileStream("hdfs://s198hadoop:9000/user/hadoop/lindefu/structuredstreaming")//在hdfs上读取数据
    ssc.checkpoint("hdfs://s198hadoop:9000/user/hadoop/lindefu/checkpoint")//实时分析结果保存到hdfs上

    val  words = lines.map(_.split(",")(2).toFloat).cache()#处理数据，以‘，’为分隔，转化为浮点型

    val  prices_more_100 = words
      .map(lines => {if(lines>=100.00)("大于100的商品",1)else("小于100的商品",1)})#判断条件
      .reduceByKey(_ + _)
    val stateDstream = prices_more_100.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
