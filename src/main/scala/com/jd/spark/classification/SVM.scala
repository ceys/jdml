package com.jd.spark.classification

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by zhengchen on 14-5-19.
 */
object SvmTrain {

  def main(args: Array[String]) {
    if (args.length != 7) {
      println("Usage: SvmTrain <master> <input_dir> <step_size> <regularization_parameter> <niters> <miniBatch> <output_dir>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SvmTrain")
    val data = MLUtils.loadLabeledData(sc, args(1))
    val model = SVMWithSGD.train(data, args(4).toInt, args(2).toDouble, args(3).toDouble, args(4).toDouble)

    sc.parallelize( model.intercept + "," + model.weights.mkString(",") )
      .saveAsTextFile(args(5))

    sc.stop()

  }

}

object SvmTest {

  def main(args: Array[String]) {

    val weightFile = args(1)
    val testFile = args(2)

    def predictPoint(dataMatrix: DoubleMatrix, weightMatrix: DoubleMatrix,
                     intercept: Double) = {
      val margin = dataMatrix.dot(weightMatrix) + intercept
      if (margin < 0) 0.0 else 1.0
    }

    val sc = new SparkContext(args(0), "SvmTest")
    val testData = MLUtils.loadLabeledData(sc, testFile)
    //val weightData = sc.textFile(weightFile).take(1).toString.split(",")
    //val Intercept = weightData(0).toDouble
    //val weights = new DoubleMatrix(weightData.slice(1, weightData.length).map( i => i.toDouble))
    val weights = new DoubleMatrix(1,16, -3.9943020957203284E-4,8.808342981660803E-4,1.8621298813475399E-19,0.003478865237125714,0.0,8.405929955219494E-4,0.001957140200283039,-0.0014836561139799356,3.1146490445756793E-4,3.114649044575682E-4,0.0014376770075506346,-0.0012925383604887365,0.001779034532091314,0.0,0.0,0.0012560064024229863)
    val Intercept = 5.590858293168167E-4
    //val tmp = data.map(l => (l.label, predictPoint(new DoubleMatrix(l.features), weights,Intercept),  l.label.toInt == predictPoint(new DoubleMatrix(l.features), weights,Intercept)))

    val test = testData.map(l => {
      val result = Array(0,0,0,0)
      val p = predictPoint(new DoubleMatrix(l.features), weights,Intercept)
      if (l.label==1.0) {
        result(0) = 1
        if (p==1.0) result(2) = 1
      }
      else if (l.label==0.0) {
        result(1) = 1
        if (p==0.0) result(3) = 1
      }
      result
    }).reduce( (a, b) => (for (i <- 0 until a.length) yield a(i)+b(i)).toArray )

    println(test(0),test(1),test(2),test(3))
  }
}