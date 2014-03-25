package com.jd.spark.recommendation.fpgrowth

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.sun.org.apache.xalan.internal.xsltc.compiler.ContainsCall

/**
 * @author LiZhaohai
 * @time 2014-02-13
 */
class FPTree(var itemCount: Map[String, Int], var coreItem: String, var frqNum: Int){
  
  private type recordstype = ArrayBuffer[Tuple2[Array[String],Int]]
  private type headtabletype = ArrayBuffer[HashMap[String, TreeNode]]
  
  def createTree(dataSet : recordstype, headTable: headtabletype, minSup: Int): TreeNode={
    val root = new TreeNode()
    
    /**
     * Initialize head table with minimum support
     */
    //println("===Begin createtree....coreItem:"+coreItem)
    val tmpheadtable = new HashMap[String, Int]()
    for((trans, count) <- dataSet){
        for(item <- trans){             
            tmpheadtable(item) = tmpheadtable.getOrElse(item, 0) + count            
        }
    }
    //Remove items less than minimum support
    for(k <- tmpheadtable.keys){
      //println("======k<- tmpheadtable.keys " +k+":"+tmpheadtable(k))
       if(tmpheadtable(k) < minSup){
          tmpheadtable.remove(k)
       }
    }
    val sortedtable = tmpheadtable.toArray.sortWith((A,B) => if (A._2 == B._2)  A._1.compareTo(B._1) < 0  else A._2 < B._2)
    for((sorteditem, itemcount) <- sortedtable){ 
        headTable += HashMap(sorteditem-> null)
    }
    //End initialize head table with minimum support
    
    for((trans, count) <- dataSet){
        val localData = new HashMap[String, Int]()
        for(item <- trans){ 
           if(tmpheadtable.contains(item)){
               localData(item) = itemCount(item)
           }
        }
        if(localData.size != 0){
          val sortedtrans = localData.toArray.sortWith((A,B) => if (A._2 == B._2)  A._1.compareTo(B._1) < 0  else A._2 > B._2).map(_._1)
          updateTree(sortedtrans, root, count, headTable)
        }
    }
    
    root
  }
  
  /*
   * Update tree
   */
  def updateTree(records: Array[String], inTree: TreeNode, count: Int, headTable:headtabletype){
    //println("====Begin updataTree... "+records.mkString(","))
     val firstitem = records.head
     if(inTree.findChild(firstitem) != null){
         inTree.findChild(firstitem).countIncrement(count)
     }
     else{
         val child = new TreeNode()
         child.setName(firstitem).setCount(count).setParent(inTree)
         if(child.getChildren == null){
             inTree.setChildren(new ArrayBuffer[TreeNode])
         }
         inTree.getChildren().append(child)
         var (headpos, hflag, i, htlen, stopflag) = (-1, false, 0, headTable.length, false)
         while((i < htlen && !stopflag)){
             if(headTable(i).contains(firstitem)){
                 if(headTable(i)(firstitem) != null){
                     hflag = true
                 }
                 headpos = i
                 stopflag = true
             }
             i = i+1
         }
         if(!hflag){
             headTable(headpos)(firstitem) = child
         }
         else{
            /*
             * Update Node Link
             */
           var oldNode = headTable(headpos)(firstitem)
             while((oldNode.getNodeLink() != null)){
                 oldNode = oldNode.getNodeLink()
             }
             oldNode.setNodeLink(child)
         }
     }
     if(records.tail.length > 0){
       updateTree(records.tail, inTree.findChild(firstitem), count, headTable)
     }    
     
  }
  
  /*
   * ascendTree
   */
  def ascendTree(leafNode: TreeNode): Array[String] = {
     //println("=====ascendTree...")
     val prefixPath = new ArrayBuffer[String]()
     var tmpNode = leafNode.copy()
     tmpNode = tmpNode.getParent()
     while(tmpNode != null){
         if(tmpNode.getName() != null){
             prefixPath += tmpNode.getName()
         }
         tmpNode = tmpNode.getParent()
     }
     prefixPath.toArray
  }
  
  /*
   * findPrefixPath
   */
  def findPrefixPath(item:String, treenode: TreeNode): recordstype = {
     //println("=====findPrefixPath...")
     val prefixPathList = new recordstype()
     var tmpNode = treenode.copy()
     while(tmpNode != null){
         prefixPathList += Tuple2(ascendTree(tmpNode), tmpNode.getCount)
         tmpNode = tmpNode.getNodeLink()
     }
     
     prefixPathList
  }
  
  
  /*
   * mineTree
   */
  def mineTree(inTree:TreeNode, ht:headtabletype, preFix:ArrayBuffer[String], 
      freqItemList:ArrayBuffer[Tuple2[ArrayBuffer[String], Int]], minSup: Int, firstFlag: Boolean){
      //println("=====mineTree...")
      var htlist = new ArrayBuffer[String]()
      
      //Only find coreItem in the headTable of the first base FP-Tree
      if(firstFlag){         
         htlist.append(coreItem)
      }
      else{        
         htlist = ht.map(_.keySet.head)         
      }
     
      for(item <- htlist){
         //deep copy of preFix         
         var newFreqSet = new ArrayBuffer[String]()
         for(p <- preFix){
            newFreqSet += p
         }
         newFreqSet += item 
         if(newFreqSet.length <= frqNum ){
            var (itempos, i, htlen, stopflag) = (-1, 0, ht.length, false)            
            while(i < htlen && !stopflag){
               if(ht(i).contains(item)){
                  itempos = i
                  stopflag = true
               }
              i = i + 1
            }
            
            
            //Only output items which starts with coreItem(That's because the FP-Tree was distributed by each item)
            if(newFreqSet.head == coreItem){           
               val cocount = ht(itempos)(item).getCount()
               freqItemList.append(Tuple2(newFreqSet, cocount))
            }
            
            
            var newsets = findPrefixPath(item,ht(itempos)(item))
            var subHeadtable = new headtabletype()
            var myCondTree = createTree(newsets, subHeadtable, minSup)
            if(subHeadtable.size != 0){
               mineTree(myCondTree, subHeadtable, newFreqSet, freqItemList, minSup, false)
            }
         }

      }
  }


}


