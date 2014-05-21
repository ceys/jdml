package com.jd.spark.classification

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by zhengchen on 14-5-19.
 */
object SVM {

  def main(args: Array[String]) {
    val numIterations = args(1)

  }

  // Load and parse the data file
  val data = sc.textFile("mllib/data/sample_svm_data.txt")
  val parsedData = data.map { line =>
    val parts = line.split(' ')
    LabeledPoint(parts(0).toDouble, parts.tail.map(x => x.toDouble).toArray)
  }

  // Run training algorithm to build the model
  val numIterations = 20
  val model = SVMWithSGD.train(parsedData, numIterations)

  // Evaluate model on training examples and compute training error
  val labelAndPreds = parsedData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
  println("Training Error = " + trainErr)

}
