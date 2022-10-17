package com.homework.dergunov

import org.apache.spark.{SparkConf, SparkContext}

object BroadCast {

  def main(args : Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Broadcast words count")

    val sparkContext = new SparkContext(conf)

    val readCompanies = sparkContext
      .textFile("/home/nikita/IdeaProjects/ML_Homework_3/Company.csv")
      .map(str => str.split(','))
      .filter(arr => arr(0) == arr(0).toUpperCase)
      .map(arr => (arr(0), arr(1)))
      .collect()

    val broadCasted = sparkContext.broadcast(readCompanies.toMap)

    sparkContext
      .textFile("/home/nikita/IdeaProjects/ML_Homework_3/Company_Tweet.csv")
      .map(str => str.split(','))
      .filter(arr => arr(0) != "tweet_id")
      .map(arr => (arr(1), arr(0)))
      .flatMap(x =>
        broadCasted.value.get(x._1) match {
          case Some(r) => Some(x._1, x._2, r)
          case None => None
        })
      .saveAsTextFile("broadcast_join")

    sparkContext.stop()
  }
}
