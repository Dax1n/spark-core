package com.daxin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by 青春常驻Dax1n on 2016/12/8.
  * action触发作业的真正执行，所以看代码要action作为入口往里面看
  *
  *
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount")

    val sc = new SparkContext(conf)

    //tranformation
    val rdd = sc.textFile("G:\\words\\3.txt") //RDD1

    //tranformation                        RDD2                 RDD3      RDD4(shffle过程)
    val tmp: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    println("tmp.toDebugString : " + tmp.toDebugString)


    //action  ,action方法里面都调用org.apache.spark.SparkContext.runJob触发作业执行
      //  tmp.count()
    //    tmp.collect()
    //RDD5存储   所以RDD5是最后一个RDD，finalRDD
    val result = tmp.saveAsTextFile("d:\\resultfile\\")


    akka.actor.Actor//400行是一个特质




    sc.stop()
  }
}
