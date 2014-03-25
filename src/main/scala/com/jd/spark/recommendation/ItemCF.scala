package com.jd.spark.recomendation

import scala.collection.immutable._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner.defaultPartitioner

class ItemCF extends GeneralizedCF with Serializable {

  def run(userItem: RDD[(Int, Int)]): RDD[String] = {
    run(userItem, defaultPartitioner(userItem).numPartitions)
  }
  def run(userItem: RDD[(Int, Int)], numPartitions: Int): RDD[String] = {
    val sc = userItem.context
    
    //RDD (user,Map(item,1))
    val userMapres = userItem.combineByKey(createCombiner(_), mergeValue(_, _), mergeCombiners(_, _), numPartitions)
    //RDD (item,Map(item,1))
    val itemMapres = userMapres.
      values.filter(f => f.keySet.size > 1).map(f => {
        for (i <- f.keySet) yield (i, f)
      }).flatMap(identity)
    // RDD (item,Map(item,sum))   scala 2.10 hashMap  merged function
    val itemComb = itemMapres.reduceByKey((m1, m2) => {
      (m1.keySet ++ m2.keySet).map(i => (i, m1.getOrElse(i, 0) + m2.getOrElse(i, 0))).toMap
    }, numPartitions)
    
    val occItems = userItem.map(line => {
      (line._2, line._1)
    })
    val occTime = occItems.combineByKey(createCombiner, mergeValue, mergeCombiners).map(f => (f._1, f._2.size)).collectAsMap
    val broadcastTimes = sc.broadcast(occTime)
    //RDD (item	item1:rate,item2:rate,.....)
    val recoRes = itemComb.map(f => {
      val m = broadcastTimes.value
      val rl = for (item <- f._2.keySet if !item.equals(f._1))
        yield (item, f._2(item) / Math.sqrt(m(f._1) * m(item)))

      ("%s\t%s".format(f._1, getTopNString(rl, 9,",")))
    })
    recoRes
  }
}
object ItemCF{
  def main(args: Array[String]) {
    if (args.length != 6) {
      println("usage is org.test.WordCount <master> <input> <output> <parallelism> <memory> <Delimiter>")
      return
    }
    
    val (master, input, output, parallelism,memory,delimiter) = (args(0), args(1), args(2), args(3),args(4),args(5))
    val conf = new SparkConf().setMaster(master).setAppName("Spark-ItemCF-Q").setSparkHome(System.getenv("SPARK_HOME"))
    conf.set("spark.default.parallelism", parallelism)
	conf.set("spark.executor.memory", memory)
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	conf.set("spark.kryo.registrator", classOf[CFRegistrator].getName)
	
	val sc = new SparkContext(conf)
    
    // Read user-item from HDFS file
    val textFile = sc.textFile(input).filter(f=>{
      val temp=f.split(delimiter) 
      (temp(1).trim()!=""&&temp(2).trim()!="")}
    )
    val train_data = textFile.map(line => {
      val fileds = line.split(delimiter)
      (fileds(1).toInt, fileds(2).toInt)
    })
    val result = new ItemCF().run(train_data, parallelism.toInt)
    
    result.saveAsTextFile(output)
    sc.stop
  }
}
