package com.jd.spark.evaluation

/**
 * Created by zhengchen on 14-5-17.
 */
abstract class  Metrics {

  def accuracyScore(): Float

  def precisionScore(): Float

  def recallScore(): Float

}
