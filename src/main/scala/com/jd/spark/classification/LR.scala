package com.jd.spark.classification

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by zhengchen on 14-5-21.
 */
object LR {

  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: LogisticRegression <master> <input_dir> <step_size> " +
        "<niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "LogisticRegression")
    val data = MLUtils.loadLabeledData(sc, args(1))
    val model = LogisticRegressionWithSGD.train(data, args(3).toInt, args(2).toDouble)
    println("Weights: " + model.weights.mkString("[", ", ", "]"))
    println("Intercept: " + model.intercept)

    sc.stop()
  }

}
