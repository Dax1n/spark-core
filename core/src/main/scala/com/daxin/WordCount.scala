package com.daxin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by 青春常驻Dax1n on 2016/12/8.
  */
object WordCount {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ReadSource").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //tranformation
    val rdd = sc.textFile("G:\\words\\3.txt")

    //tranformation
    val tmp: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    println("tmp.toDebugString : "+tmp.toDebugString)


    //action
    val result = tmp.saveAsTextFile("d:\\resultfile\\")


    sc.stop()
  }
}
