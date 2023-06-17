import org.apache.spark.ml.PipelineModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object LogisticClass {
    def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setAppName("LogisticClass")
//        .setMaster("local[*]")
      val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
      val sc: SparkContext = spark.sparkContext
      sc.setLogLevel("WARN")
      import spark.implicits._
      //加载文件
      val df = spark.read.format("csv")
        .option("header", "true")
        .load("hdfs://s198hadoop:9000/user/hadoop/taidicup/taidibei_toTest.csv")
        .cache()
      //导入模型
      val model = PipelineModel.load("hdfs://s198hadoop:9000/user/hadoop/cvModel")
      val result = model.transform(df)
      result.select("id","prediction").show(false)
  }
}
