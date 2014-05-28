package com.jd.spark.evaluation

import org.apache.spark.rdd.RDD

/**
 * Created by zhengchen on 14-5-18.
 */
object RecommendationMetrics {

  /**
   * 将推荐结果与测试数据，按用户id做join操作
   *
   * @param modelData 推荐数据 (userid, itemid, score)
   * @param testData 测试数据 (userid, itemid)
   * @param SEP1 推荐数据分隔符,默认为","
   * @param SEP2 测试数据分隔符,默认为"\t"
   * @return (userid, (recItemSortedSeq, testItemSet))
   */
  def joinRecAndTest(modelData:RDD, testData:RDD, SEP1:String=",", SEP2:String="\t"): RDD = {

    val r1 = modelData.map(line => {
      val ls = line.split(SEP1)
      (ls(0).toInt,(ls(1).toInt,ls(2).toFloat))
    }).groupByKey().map{ case (k,v) => (k,v.sortWith( (t1, t2) => t1._2 > t2._2) ) }

    val r2 = testData.map(line => {
        val ls = line.split(SEP2)
        (ls(0).toInt,ls(1).toInt)
    }).groupByKey()

    r1.join(r2)

  }

  /**
   * 计算准确率：推荐命中商品数/实际推荐商品数。
   * 召回率：推荐命中商品数/测试商品数
   *
   * @param groupData joinRecAndTest方法返回的RDD
   * @param topN 推荐上限值
   * @return (召回率，准确率)
   */
  def recallAndPrecision(groupData:RDD, topN:Int):Tuple2 = {

    val (hit, testNum, recNum) =
      groupData.map{ case (user, (mItems, tItems)) =>
        var count = 0
        // 计算准确率：推荐命中商品数/实际推荐商品数, topN为推荐上限值
        val precNum = Math.min(topN, mItems.length)
        for (i <- 0 until precNum)
          if (tItems.contains(mItems(i)._1))
            count += 1
        (count, tItems.length, precNum) }.reduce( (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3) )

    (hit.toFloat/testNum, hit.toFloat/recNum)

  }

}
