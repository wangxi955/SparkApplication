
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, NaiveBayes}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{FeatureHasher, StringIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}


object LogisticCVTrainModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LogisticCVModel")
//      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("hdfs://s198hadoop:9000/user/hadoop/taidicup/taidibei.csv")
      .withColumn("status", col("status").cast("Integer"))
      .withColumnRenamed("status","label")
      .cache()

    val featureHasher = new FeatureHasher()
      .setInputCols(df.columns.filter(x => (!x.equals("id") && !x.equals("label"))))
      .setOutputCol("features")

    //使用逻辑回归算法作为评估器
    val logisticRegression = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
    //构建Pipeline,包含两个流水线阶段featureHasher和logisticRegression
    val pipeline = new Pipeline().setStages(Array(featureHasher,logisticRegression))
    //设置参数网格,maxIter为最大迭代次数, regParam为正则化参数
    val paramGrid = new ParamGridBuilder()
      .addGrid(logisticRegression.maxIter,Array(60,80,100))
      .addGrid(logisticRegression.regParam,Array(0.1,0.2,0.3))
      .build()
    //用交叉验证选择模型,共有27个模型将被训练
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val cvModel = cv.fit(df)
    //获取最优的Logistic模型,保存模型后,输出具体参数
    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    bestModel.write.save("hdfs://s198hadoop:9000/user/hadoop/cvModel")
    val TDBModel = bestModel.stages.last.asInstanceOf[LogisticRegressionModel]
    println(TDBModel.explainParam(TDBModel.regParam))
    println(TDBModel.explainParam(TDBModel.maxIter))
  }
}
