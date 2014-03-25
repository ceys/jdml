package com.jd.spark.recommendation.fpgrowth

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import java.text.DecimalFormat


/**
 * This algorithm not only a pure FP-growth, but also integrated association rule mining.
 * First using FP-growth find frequent sets, then mining association rules, only find pattern for x -> {x1,x2,...}
 * Using item x, can recommend {x1,x2...}
 * 
 */

/**
 * @param delimiter: delimiter of input transaction sets
 * @param min_sup: minimum support
 * @param frq_num: maximum frequent sets
 * @param topn: top n results order by confidence
 * @param alpha: threshold for integrated n frequent sets and n-1 frequent sets. Frequent number has highest priority.
 *        eg. {a,b,c,d:0.03},{f,g,h:0.1},{g,k:0.4}. If alpha=0.001, then result will be: {a,b,c,d,f,g,h,k}
 * @param itemcount_reduce_num: reduce tasks of itemcount
 * @param fp_reduce_num: reduce tasks of fp_reduce
 */
class FPGrowth private (
      var delimiter: String,
      var min_sup: Int,
      var frq_num: Int,
      var topn : Int,
      var alpha: Double,
      var itemcount_reduce_num: Int,
      var fp_reduce_num: Int)
  extends Serializable with Logging
{
  def this() = this(",", 2, 2, 9, 0.001, 50, 50)
  
  def setDelimiter(delimiter: String): FPGrowth={
     this.delimiter = delimiter
     this
  }
  
  def setMin_sup(min_sup: Int): FPGrowth={
     this.min_sup = min_sup
     this
  }
  
  def setFrq_num(frq_num: Int): FPGrowth={
     this.frq_num = frq_num
     this
  }
  
  def setTopn(topn: Int): FPGrowth={
     this.topn = topn
     this
  }
  
  def setAlpha(alpha: Double): FPGrowth={
     this.alpha = alpha
     this
  }
  
  def setItemcount_reduce_num(itemcount_reduce_num: Int): FPGrowth={
     this.itemcount_reduce_num = itemcount_reduce_num
     this
  }
  
  def setFp_reduce_num(fp_reduce_num: Int): FPGrowth={
     this.fp_reduce_num = fp_reduce_num
     this
  }
  
  def run(data: RDD[String]): RDD[String]={
    
    val sc = data.sparkContext
   //Load Original data and persist
    val oriData = data.map(line => line.split(delimiter).toSet).filter(_.size >1).cache()  
    
    
    /*
     * Item count task, filtering those count >= min_sup, and change it to a Map
     */    
    val itemCountList = oriData.flatMap(l=>l).map(word => (word,1)).reduceByKey(_ + _, itemcount_reduce_num).collect().filter(
         x => x._2 >= min_sup).toMap
   
    //Broadcast the itemCountList
    val BROAD_itemCount = sc.broadcast(itemCountList)
    
    /*
     * Distributing data
     */
    val distributedData = oriData.flatMap( itemlist => { 
       var distributeLine = new ArrayBuffer[Tuple2[String,Array[String]]]()
       val tmpMap = new HashMap[String,Int]()
       for(item <- itemlist){
          if(BROAD_itemCount.value.contains(item)){
             tmpMap(item)=BROAD_itemCount.value(item).toInt
          }
       }
       val sortedItems = tmpMap.toArray.sortWith((A,B) => if (A._2 == B._2)  A._1.compareTo(B._1) < 0  else A._2 > B._2).map(_._1)
       var (sorIteLen, i) = (sortedItems.length, 1)
       while(i < sorIteLen){
         distributeLine += Tuple2(sortedItems(i), sortedItems.slice(0, i+1))
         //println(sortedItems.mkString(",")+"=="+sortedItems(i)+":"+sortedItems.slice(0, i+1).mkString(","))
         i = i + 1
       }
       distributeLine
      }
    ).groupByKey(fp_reduce_num)
    
    //distributedData.foreach(println)

    
   //BROAD_itemCount.value.foreach(println)
    val fpgrowthdata = distributedData.flatMap(groupdata => {
        val itemname = groupdata._1
        //get dataset
        var datasets = new ArrayBuffer[Tuple2[Array[String],Int]]()
        for(oneTrans <- groupdata._2){
            datasets += Tuple2(oneTrans, 1)
            //println("--oneTrans:itemname-"+itemname+":"+oneTrans.mkString(","))
        }
        
        val fptree = new FPTree(BROAD_itemCount.value, itemname, frq_num);
        var headTable = new ArrayBuffer[HashMap[String, TreeNode]]()
        var intTree = fptree.createTree(datasets, headTable, min_sup)
        var preFix = new ArrayBuffer[String]()
        var freqItemList = new ArrayBuffer[Tuple2[ArrayBuffer[String], Int]]()
        
        if(headTable.length >0){
          fptree.mineTree(intTree, headTable, preFix, freqItemList, min_sup, true)
        }        
        //println("===End of mineTree..."+freqItemList.length)
        type resulttype = (String, Tuple1[String])
        var result = new ArrayBuffer[resulttype]()
        val formatter = new DecimalFormat("#.###")
        for((itemslist,count)<- freqItemList){
            var (i,tblen)=(0,itemslist.length)
            //println("===frqItemList: "+itemslist.mkString(",")+" "+count)
            if(tblen >1){
                while(i< tblen){
		           var str= itemslist.slice(0, i).mkString(",")+","+itemslist.slice(i+1, tblen).mkString(",")
		           str = str.split(",").filter(_ != "").mkString(",")		        
		           result += Tuple2(itemslist(i), Tuple1(str + "\t" + formatter.format(count.toDouble/BROAD_itemCount.value(itemslist(i)))))
		           //println("subresult: "+itemslist(i)+" -> "+str + "\t" + formatter.format(count.toDouble/BROAD_itemCount.value(itemslist(i))))
		           i = i + 1
		        }
            }

        }
     result
     }
    ).groupByKey(fp_reduce_num)
    
    //fpgrowthdata.foreach(println)
   
    /*
     * get top n result
     */
    val topNresult = fpgrowthdata.flatMap(anItem =>{
        val itemname = anItem._1
        val frqlist = anItem._2
        var finalresult = new ArrayBuffer[String]()
        val soretdlist=frqlist.sortWith((AA,BB) => { 
              val A=AA._1
              val B=BB._1
              val allen = A.split("\t")(0).split(",").length
              val bllen = B.split("\t")(0).split(",").length
              if(allen != bllen) allen > bllen
              else{
                 A.split("\t")(1).toDouble > B.split("\t")(1).toDouble
              }
            })
            
        var tmpresult = new ArrayBuffer[String]()        
        for(aline <- soretdlist){
            //println("-->>> "+itemname+":"+aline._1)
            var addflag = false
            if(aline._1.split("\t")(0).split(",").length >2){
               if(aline._1.split("\t")(1).toDouble >= alpha){
                  addflag=true
               }
            }
            else{
               addflag=true
            }
            if(addflag){
               for(item <- aline._1.split("\t")(0).split(",")){
                  if(tmpresult.size < topn && !tmpresult.contains(item)){
                      tmpresult += item
                      //finalresult += itemname + "\t" + item + "\t" + aline._1.split("\t")(1)
                  }
               }
            }


        }
        finalresult += itemname + "\t"+ tmpresult.mkString(",")
        finalresult
    })
    topNresult
    //topNresult.foreach(println)    
  }
}

object FPGrowth {
  
  def run(
      data: RDD[String],
      delimiter: String,
      min_sup: Int,
      frq_num: Int,
      topn : Int,
      alpha: Double,
      itemcount_reduce_num: Int,
      fp_reduce_num: Int ): RDD[String]=
  {
     new FPGrowth()
     .setDelimiter(delimiter)
     .setMin_sup(min_sup)
     .setFrq_num(frq_num)
     .setTopn(topn)
     .setAlpha(alpha)
     .setItemcount_reduce_num(itemcount_reduce_num)
     .setFp_reduce_num(fp_reduce_num)
     .run(data)
  }
  
  def main(args: Array[String]) {
    
    if (args.length != 10) {
      println("Usage: FPJob <master><input_file><output_file> <delimiter> <min_sup> <frq_num><topn> <alpha><itemcount_reduce_num> <fp_reduce_num>")
      System.exit(1)
    }
    
    val (master, inputFile, outputFile, delimiter, min_sup, frq_num, topn, alpha, itemcount_reduce_num, fp_reduce_num) = (args(0), args(1),args(2), args(3), args(4).toInt, args(5).toInt, args(6).toInt, args(7).toDouble, args(8).toInt,args(9).toInt)
    
    val sc = new SparkContext(master, "FPgrowth",System.getenv("SPARK_HOME"))
    
    System.setProperty("spark.executor.memory", "1g")

    val dataFile = sc.textFile(inputFile)
    val topNresult = FPGrowth.run(dataFile, delimiter, min_sup, frq_num, topn, alpha, itemcount_reduce_num, fp_reduce_num)
    
    //write result to output file
    topNresult.saveAsTextFile(outputFile)
    //topNresult.foreach(println)

  }
}
