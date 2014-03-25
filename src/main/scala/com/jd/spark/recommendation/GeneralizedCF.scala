package com.jd.spark.recommendation

import scala.collection.immutable._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

trait  GeneralizedCF extends Serializable{
  
  /**
   * createCombiner  used by  combineByKey
   */
  def createCombiner(s: Int) = HashMap[Int, Int](s -> 1)
  /**
   * mergeValue used by  combineByKey
   */
  def mergeValue(m: Map[Int, Int], n: Int) = (m + (n -> 1))
  /**
   * mergeCombiners  used by  combineByKey
   */
  def mergeCombiners(m1: Map[Int, Int], m2: Map[Int, Int]) = m1 ++ m2 
  /**
   * get two number sum 
   */
  def addv(v1: Double, v2: Double) = v1 + v2

 
  /**
   * merge two map with function 
   */
  def mergeMap(map1: Map[Int, Double], map2: Map[Int, Double], func: (Double, Double) => Double) = {
    (map1.keySet ++ map2.keySet).map(mm => (mm, func(map1.getOrElse(mm, 0.0), map2.getOrElse(mm, 0.0)))).toMap
  }
  
   /**
   * TraversableOnce[(key,value)] order by value desc
   * get top n and connect with connector 
   * return :String
   */
  def getTopNString(xs: TraversableOnce[(Int, Double)], n: Double,connector:String) = {
    var ss = getTopNList(xs,n)
    val st = for (f <- ss) yield (f._1 + ":" + f._2)
    st.mkString(connector)
  }
  
  /**
   * TraversableOnce[(key,value)] order by value desc
   * return :List
   */
  def getTopNList(xs: TraversableOnce[(Int, Double)], n: Double) = {
      var ss = List[(Int, Double)]()
      var min = Double.MaxValue
      var len = 0
      xs foreach { e =>
        if (len < n || e._2 > min) {
          ss = (e :: ss).sortWith(_._2 < _._2)
          min = ss.head._2
          len += 1
        }
        if (len > n) {
          ss = ss.tail
          min = ss.head._2
          len -= 1
        }
      }
      ss.reverse
    }
  
}
