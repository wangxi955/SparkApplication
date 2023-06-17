
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, NaiveBayes}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{FeatureHasher, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object LogisticTrainModel {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("LogisticModel")
//          .master("local[*]")
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

        val Array(training,test) = df.randomSplit(Array(0.8,0.2))

        val featureHasher = new FeatureHasher()
              .setInputCols(df.columns.filter(x => (!x.equals("id") && !x.equals("label"))))
              .setOutputCol("features")

        //使用逻辑回归算法作为评估器
        val logisticRegression = new LogisticRegression()
            .setFeaturesCol("features")
            .setLabelCol("label")
        //构建Pipeline,包含两个流水线阶段featureHasher和logisticRegression
        val pipeline = new Pipeline().setStages(Array(featureHasher,logisticRegression))

        val model = pipeline.fit(training)
//        model.write.overwrite().save("hdfs://s198hadoop:9000/user/hadoop/Model")
        val result = model.transform(test)
        result.select("label","prediction").show(false)
        val metrics = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").getMetrics(result)

        // 正确率
        System.out.println("accuracy:" + metrics.accuracy)
        // 精确率
        System.out.println("precision1:" + metrics.precision(1))
        System.out.println("precision0:" + metrics.precision(0))
        // 召回率
        System.out.println("recall1:" + metrics.recall(1))
        System.out.println("recall0:" + metrics.recall(0))
        // F度量
        System.out.println("fMeasure1:" + metrics.fMeasure(1))
        System.out.println("fMeasure0:" + metrics.fMeasure(0))

    }
}
