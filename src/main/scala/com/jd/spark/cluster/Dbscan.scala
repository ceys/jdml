package com.jd.spark.cluster

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Created by zhengchen on 14-5-12.
 */

object Dbscan {

  def run(df: RDD[String], eps: Float, minpts: Int, sep: String) = {

    //输入：userid userid similarity
    val edges = df.filter(line => line.split(sep)(2).toFloat >= eps)
      .map{line => {val cons=line.split(sep); (cons(0).toLong,cons(1).toLong)}}

    //用epsilon阈值和minpoints过滤，生成graph。
    val graph = Graph.fromEdgeTuples(edges,1)

    //在graph上找connected component
    graph.connectedComponents().vertices

  }


  def main(args: Array[String]) {

    val master = args(0)
    val eps = args(1).toFloat
    val minpts = args(2).toInt
    val inputFile = args(3)
    val outputFile = args(4)
    val sep = args(5)

    if (args.length == 0) {
      System.err.println("Usage: Dbscan <master> [<slices>]")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("Dbscan")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    val df = sc.textFile(inputFile)
    val cc = run(df, eps, minpts, sep)
    cc.saveAsTextFile(outputFile)
  }

}
