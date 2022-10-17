## Практическое задание

#### 1. Какие плюсы и недостатки у Merge Sort Join в отличии от Hash Join? (1 балл)
При join необходимо, чтобы одинаковые ключи с разных rdd оказались на одних и тех же воркерах, 
считается hash коде и по нему определяется на какой воркер необходимо перекинуть запись, и как раз операция объединения 
при Hash Join происходит в зависимости от hash ключа за линейнок время O(n) однако он работает не со всми типами данных,
при Merge Sort Join используется сортировка слиянием из-за чего сложность O(n*log(n)) однако он более униеверсален и
используется по умолчанию
#### 2. Соберите WordCount приложение, запустите на датасете ppkm_sentiment (1 балл)
#### 3. Измените WordCount так, чтобы он удалял знаки препинания и приводил все слова к единому регистру (1 балл)
#### 4. Измените выход WordCount так, чтобы сортировка была по количеству повторений, а список слов был во втором столбце, а не в первом (1 балл)
#### 5. Измените выход WordCount, чтобы формат соответствовал TSV (1 балл)
#### 6. Добавьте в WordCount возможность через конфигурацию задать список стоп-слов, которые будут отфильтрованы во время работы приложения (2 балла)
```sh
sbt pakage
sudo docker cp /home/nikita/IdeaProjects/ML_Homework_3/target/scala-2.11/ml_homework_3_2.11-0.1.0-SNAPSHOT.jar  0fa82b0b1f16:/home/hduser/
sudo docker cp ppkm 0fa82b0b1f16:/home/hduser/ /* Копирование датасета в контейнер */
```

```sh 
hdfs dfs -put ppkm /user/hduser
spark-submit --class com.homework.dergunov.WordCount --master yarn --deploy-mode cluster ml_homework_3_2.11-0.1.0-SNAPSHOT.jar
```
```scala
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
```

Результаты (tail)
```sh
33	#covid19
36	covid19
39	perpanjangan
43	pembatasan
44	kegiatan
58	yg
60	#ppkmmikro
65	masyarakat
110	mikro
118	ppkm
```

#### 7. Выполните broadcast join на двух датасетах, не используя метод join(). Можно использовать любые предварительные трансформации. В качестве исходных данных возьмите Company.csv и Company_Tweet.csv из датасета Tweets about the Top Companies from 2005 to 2020 (3 балла)
```scala
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
```

Результаты (пример)
```sh
(TSLA,1084852863887495168,Tesla Inc)
(TSLA,1084853369347428352,Tesla Inc)
(TSLA,1084853371943772160,Tesla Inc)
(TSLA,1084853373269086209,Tesla Inc)
(TSLA,1084853388058247168,Tesla Inc)
(TSLA,1084853514814345218,Tesla Inc)
(TSLA,1084853622913945602,Tesla Inc)
```