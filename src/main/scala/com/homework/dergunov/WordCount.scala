package com.homework.dergunov

import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

object WordCount {
  def main(args : Array[String]): Unit = {
    val conf: SparkConf  = new SparkConf()
      .setAppName("Spark words count")

    val inputPath = conf.get(Config.IN_PATH_PARAM, Config.IN_PATH_DEFAULT)
    val outputPath = conf.get(Config.OUT_PATH_PARAM, Config.OUT_PATH_DEFAULT)

    val stopWordsStrPath = conf.get(Config.STOP_WORDS_PARAM, Config.STOP_WORDS_DEFAULT)
    val sparkContext = new SparkContext(conf)

    val stopWordsSet = getStopWords(stopWordsStrPath, sparkContext)

    sparkContext
      .textFile(inputPath)
      .flatMap(str => str split "\\s")
      .map(word => word.trim)
      .map(word => word.replaceAll("[.,;:\"()!?-]", ""))
      .filter(word => word.nonEmpty)
      .filter(word => !stopWordsSet.contains(word))
      .map(word => word toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey()
      .map(tuple => s"${tuple._1}\t${tuple._2}")
      .repartition(1)
      .saveAsTextFile(outputPath)

    sparkContext.stop()
  }

  def getStopWords(path: String, sparkContext: SparkContext): Set[String] = {
    sparkContext
      .textFile(path)
      .flatMap(str => str split "\\s")
      .collect()
      .toSet
  }

  object Config {
    val IN_PATH_PARAM: String = "scala.word.count.input"
    val IN_PATH_DEFAULT: String = "/user/hduser/ppkm/*.csv"

    val OUT_PATH_PARAM: String = "scala.word.count.output"
    val OUT_PATH_DEFAULT: String = "/user/hduser/spark/ppkm_out"

    val STOP_WORDS_PARAM = "scala.word.count.stop.words"
    val STOP_WORDS_DEFAULT = "/user/hduser/ppkm/stopwordv1.txt"
  }
}
