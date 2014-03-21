package com.jd.spark.recomendation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object MF {
    def main(args: Array[String]) {
        val master = args(0)
        val inputfile = args(1)
        val outputfile = args(2)
        val numIterations = args(3).toInt
        val rank = args(4).toInt
        val lambda = args(5).toFloat
        val block = args(6).toInt

        // Load and parse the data
        val conf = new SparkConf()
                     .setMaster(master)
                     .setAppName("MF")
                     .set("spark.executor.memory", "2g")
        val sc = new SparkContext(conf)
        //val data = sc.textFile("/tmp/mf/item_score.data")
        val data = sc.textFile(inputfile)
        val ratings = data.map(_.split('\t') match {
            case Array(user, item, rate) =>  Rating(user.toInt, item.toInt, rate.toDouble)
        })
        
        // Build the recommendation model using ALS
        val model = ALS.train(ratings, rank, numIterations, lambda, block)
        
        // Evaluate the model on rating data
        val usersProducts = ratings.map{ case Rating(user, product, rate)  => (user, product)}
        val predictions = model.predict(usersProducts).map{
            case Rating(user, product, rate) => ((user, product), rate)
        }
        predictions.saveAsTextFile(outputfile)
        val ratesAndPreds = ratings.map{
            case Rating(user, product, rate) => ((user, product), rate)
        }.join(predictions)
        val MSE = ratesAndPreds.map{
            case ((user, product), (r1, r2)) =>  math.pow((r1- r2), 2)
        }.reduce(_ + _)/ratesAndPreds.count
        println("Mean Squared Error = " + MSE)
    }
}
