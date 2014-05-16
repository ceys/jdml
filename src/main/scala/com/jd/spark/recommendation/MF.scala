package com.jd.spark.recommendation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

object MF {

  private class ALSRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Rating])
    }
  }

  def main(args: Array[String]) {

    if (args.length < 5 || args.length > 10) {
      println("Usage: ALS <master> <ratings_file> <rank> <iterations> <output_dir> " +
        "[<lambda>] [<implicitPrefs>] [<alpha>] [<blocks>]")
      System.exit(1)
    }
    val (master, ratingsFile, rank, iters, outputDir) =
      (args(0), args(1), args(2).toInt, args(3).toInt, args(4))
    val lambda = if (args.length >= 6) args(5).toDouble else 0.01
    val implicitPrefs = if (args.length >= 7) args(6).toBoolean else false
    val alpha = if (args.length >= 8) args(7).toDouble else 1
    val blocks = if (args.length >= 9) args(8).toInt else -1
    val sep = if (args.length == 9) args(9) else " "
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator",  classOf[ALSRegistrator].getName)
      .set("spark.kryo.referenceTracking", "false")
      .set("spark.kryoserializer.buffer.mb", "8")
      .set("spark.locality.wait", "10000")
    val sc = new SparkContext(master, "ALS", conf)

    val ratings = sc.textFile(ratingsFile).map { line =>
      val fields = line.split(sep)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }
    /*
    val model = new ALS(rank = rank, iterations = iters, lambda = lambda,
      numBlocks = blocks, implicitPrefs = implicitPrefs, alpha = alpha).run(ratings)
    */
    val model = ALS.trainImplicit(ratings, rank, iters, lambda, blocks, alpha)
    /*
    if (implicitPrefs) {
      val model = ALS.trainImplicit(ratings, rank, iters, lambda, blocks, alpha)
    }
    else {
      val model = ALS.train(ratings, rank, iters, lambda, blocks)
    }
    */
    model.userFeatures.map{ case (id, vec) => id + "," + vec.mkString(" ") }
      .saveAsTextFile(outputDir + "/userFeatures")
    model.productFeatures.map{ case (id, vec) => id + "," + vec.mkString(" ") }
      .saveAsTextFile(outputDir + "/productFeatures")
    println("Final user/product features written to " + outputDir)

    // Evaluate the model on rating data
    val usersProducts = ratings.map{ case Rating(user, product, rate)  => (user, product)}
    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rate) => user.toString + sep + product.toString + sep + rate.toString
    }
    predictions.saveAsTextFile(outputDir + "/predictions")
    /*
    val ratesAndPreds = ratings.map{
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map{
      case ((user, product), (r1, r2)) =>  math.pow((r1- r2), 2)
    }.reduce(_ + _)/ratesAndPreds.count
    println("Mean Squared Error = " + MSE)
    */
  }
}
