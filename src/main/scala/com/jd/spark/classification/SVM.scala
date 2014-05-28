package com.jd.spark.classification

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by zhengchen on 14-5-19.
 */
object SVM {

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: SVM <master> <input_dir> <step_size> <regularization_parameter> <niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SVM")
    val data = MLUtils.loadLabeledData(sc, args(1))
    val model = SVMWithSGD.train(data, args(4).toInt, args(2).toDouble, args(3).toDouble)
    println("Weights: " + model.weights.mkString("[", ", ", "]"))
    println("Intercept: " + model.intercept)

    sc.stop()
  }

}
