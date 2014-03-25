package com.jd.spark.recommendation.fpgrowth

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer

/**
 * @author: LiZhaohai
 * @create date: 2014-01-20
 */
case class TreeNode (
    private var name: String,
    private var count: Int,
    private var parent: TreeNode,
    private var children: ArrayBuffer[TreeNode],
    private var nodeLink: TreeNode) 
{
    def this() = this(null,0,null,new ArrayBuffer[TreeNode],null)
    
	def setName(name: String): TreeNode={
	  this.name = name
	  this
	}
	def getName(): String={
	  this.name
	}
	
	def setCount(count: Int): TreeNode={
	  this.count = count
	  this
	}
	def getCount(): Int={
	  this.count
	}
	
	def setParent(parent: TreeNode): TreeNode={
	  this.parent = parent
	  this
	}
	def getParent(): TreeNode={
	  this.parent
	}
	
	def setChildren(children: ArrayBuffer[TreeNode]): TreeNode={
	  this.children = children
	  this
	}
	def getChildren(): ArrayBuffer[TreeNode]={
	  this.children
	}
	
	def setNodeLink(nodeLink: TreeNode): TreeNode={
	  this.nodeLink = nodeLink
	  this
	}
	def getNodeLink(): TreeNode={
	  this.nodeLink
	}
	
	def countIncrement(numOccur: Int){
	  this.count += numOccur
	}
	
	/**
	 * Add a child
	 */
	def addChild(child: TreeNode) {
	  this.children += child
	}
    
	/**
	 * Find if exists the item or not
	 */
	def findChild(nodeName: String): TreeNode={
	  for(child <- children){
	    if(child.name.equals(nodeName)){
	      return child
	    }
	  }
	  return null
	}
  
}
